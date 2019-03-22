/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.replication.session.OperationContext;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

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
    private final AvailabilityGuard globalAvailabilityGuard;
    private final Log log;
    private final ClusteredDatabaseManager<?> databaseManager;
    private final ReplicationMonitor replicationMonitor;
    private final long availabilityTimeoutMillis;
    private final LeaderProvider leaderProvider;

    public RaftReplicator( LeaderLocator leaderLocator, MemberId me, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            LocalSessionPool sessionPool, ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy,
            long availabilityTimeoutMillis, AvailabilityGuard globalAvailabilityGuard, LogProvider logProvider,
            ClusteredDatabaseManager<?> databaseManager, Monitors monitors )
    {
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.progressTimeoutStrategy = progressTimeoutStrategy;
        this.availabilityTimeoutMillis = availabilityTimeoutMillis;
        this.globalAvailabilityGuard = globalAvailabilityGuard;
        this.log = logProvider.getLog( getClass() );
        this.databaseManager = databaseManager;
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
                if ( command instanceof CoreReplicatedContent )
                {
                    String databaseName = ((CoreReplicatedContent) command).databaseName();
                    assertDatabaseAvailable( databaseName );
                }
                else
                {
                    assertAllDatabasesAvailable();
                }
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

    private void assertDatabaseAvailable( String databaseName ) throws UnavailableException
    {
        var databaseId = new DatabaseId( databaseName );
        Optional<DatabaseAvailabilityGuard> databaseAvailabilityGuard = databaseManager.getDatabaseContext( databaseId )
                .map( DatabaseContext::database )
                .map( Database::getDatabaseAvailabilityGuard );

        databaseAvailabilityGuard.orElseThrow( IllegalStateException::new ).await( availabilityTimeoutMillis );
        databaseManager.assertHealthy( databaseId, IllegalStateException.class );
    }

    private void assertAllDatabasesAvailable() throws UnavailableException
    {
        globalAvailabilityGuard.await( availabilityTimeoutMillis );
        databaseManager.getAllHealthServices().assertHealthy( IllegalStateException.class );
    }
}
