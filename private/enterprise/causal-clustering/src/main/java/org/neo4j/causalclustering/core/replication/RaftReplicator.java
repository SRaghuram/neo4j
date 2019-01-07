/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
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
    public Future<Object> replicate( ReplicatedContent command, boolean trackResult ) throws ReplicationFailureException
    {
        MemberId currentLeader = leaderProvider.currentLeader();
        if ( currentLeader == null )
        {
            throw new ReplicationFailureException( "Replication aborted since no leader was available" );
        }
        return replicate0( command, trackResult, currentLeader );
    }

    private Future<Object> replicate0( ReplicatedContent command, boolean trackResult, MemberId leader ) throws ReplicationFailureException
    {
        replicationMonitor.startReplication();
        try
        {
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
                        break;
                    }
                    progressTimeout.increment();
                    leader = leaderProvider.awaitLeader();
                }
            }
            catch ( InterruptedException e )
            {
                progressTracker.abort( operation );
                throw new ReplicationFailureException( "Interrupted while replicating", e );
            }

            BiConsumer<Object,Throwable> cleanup = ( ignored1, ignored2 ) -> sessionPool.releaseSession( session );

            if ( attempts > 1 )
            {
                log.info( format( "Successfully replicated after attempt %d: %s", attempts, operation ) );
            }

            if ( trackResult )
            {
                progress.futureResult().whenComplete( cleanup );
            }
            else
            {
                cleanup.accept( null, null );
            }
            replicationMonitor.successfulReplication();
            return progress.futureResult();
        }
        catch ( Throwable t )
        {
            replicationMonitor.failedReplication( t );
            throw t;
        }
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

    private void assertDatabaseAvailable() throws ReplicationFailureException
    {
        databaseService.assertHealthy( ReplicationFailureException.class );
        try
        {
            availabilityGuard.await( availabilityTimeoutMillis );
        }
        catch ( UnavailableException e )
        {
            throw new ReplicationFailureException( "Database is not available, transaction cannot be replicated.", e );
        }
    }
}
