/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.replication.session.OperationContext;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator implements Replicator, LeaderListener
{
    private final MemberId me;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final TimeoutStrategy progressTimeoutStrategy;
    private final AvailabilityGuard availabilityGuard;
    private final Log log;
    private final DatabaseService databaseService;
    private final ReplicationMonitor replicationMonitor;
    private final long availabilityTimeoutMillis;
    private final LeaderProvider leaderProvider;

    public RaftReplicator( LeaderLocator leaderLocator, MemberId me, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LocalSessionPool sessionPool,
            ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy, long availabilityTimeoutMillis, AvailabilityGuard availabilityGuard,
            LogProvider logProvider, DatabaseService databaseService, Monitors monitors )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.progressTimeoutStrategy = progressTimeoutStrategy;
        this.availabilityTimeoutMillis = availabilityTimeoutMillis;
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog( getClass() );
        this.databaseService = databaseService;
        this.replicationMonitor = monitors.newMonitor( ReplicationMonitor.class );
        this.leaderProvider = new LeaderProvider();
        leaderLocator.registerListener( this );
    }

    @Override
    public Result replicate( ReplicatedContent command ) throws ReplicationFailureException
    {
        MemberId currentLeader = leaderProvider.currentLeader();
        if ( currentLeader == null )
        {
            throw new ReplicationFailureException( "Replication aborted since no leader was available" );
        }
        return replicate0( command, currentLeader );
    }

    private Result replicate0( ReplicatedContent command, MemberId leader ) throws ReplicationFailureException
    {
        replicationMonitor.startReplication();

        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        Progress progress = progressTracker.start( operation );

        TimeoutStrategy.Timeout progressTimeout = progressTimeoutStrategy.newTimeout();
        int attempts = 0;
        try
        {
            while ( true )
            {
                attempts++;
                if ( attempts > 1 )
                {
                    log.info( format( "Replication attempt %d to leader %s: %s", attempts, leader, operation ) );
                }
                replicationMonitor.replicationAttempt();
                assertDatabaseAvailable();
                // blocking at least until the send has succeeded or failed before retrying
                outbound.send( leader, new RaftMessages.NewEntry.Request( me, operation ), true );
                progress.awaitReplication( progressTimeout.getMillis() );
                if ( progress.isReplicated() )
                {
                    if ( attempts > 1 )
                    {
                        log.info( format( "Successfully replicated after attempt %d: %s", attempts, operation ) );
                    }
                    break;
                }
                progressTimeout.increment();
                leader = leaderProvider.awaitLeader();
            }
            progress.awaitResult();
            replicationMonitor.successfulReplication();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            handleError( operation, e );
        }
        catch ( Throwable t )
        {
            handleError( operation, t );
        }
        sessionPool.releaseSession( session );
        return progress.result();
    }

    private void handleError( DistributedOperation operation, Throwable t ) throws ReplicationFailureException
    {
        progressTracker.abort( operation );
        replicationMonitor.failedReplication( t );
        throw new ReplicationFailureException( "Failure during replication", t );
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        progressTracker.triggerReplicationEvent();
        MemberId newLeader = leaderInfo.memberId();
        MemberId oldLeader = leaderProvider.currentLeader();
        if ( newLeader == null && oldLeader != null )
        {
            log.info( "Lost previous leader '%s'. Currently no available leader", oldLeader );
        }
        else if ( newLeader != null && oldLeader == null )
        {
            log.info( "A new leader has been detected: '%s'", newLeader );
        }
        leaderProvider.setLeader( newLeader );
    }

    private void assertDatabaseAvailable() throws UnavailableException
    {
        databaseService.assertHealthy( IllegalStateException.class );
        availabilityGuard.await( availabilityTimeoutMillis );
    }
}
