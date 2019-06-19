/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.replication.session.OperationContext;
import com.neo4j.causalclustering.core.state.Result;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy.Timeout;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.time.Duration;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator implements Replicator, LeaderListener
{
    private final DatabaseId databaseId;
    private final MemberId me;
    private final Outbound<MemberId,RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final TimeoutStrategy progressTimeoutStrategy;
    private final Log log;
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final ReplicationMonitor replicationMonitor;
    private final long availabilityTimeoutMillis;
    private final LeaderProvider leaderProvider;

    // TODO: Get rid of dependency on database manager!
    public RaftReplicator( DatabaseId databaseId, LeaderLocator leaderLocator, MemberId me, Outbound<MemberId,RaftMessage> outbound,
            LocalSessionPool sessionPool, ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy, long availabilityTimeoutMillis,
            LogProvider logProvider, DatabaseManager<ClusteredDatabaseContext> databaseManager, Monitors monitors, Duration leaderAwaitDuration )
    {
        this.databaseId = databaseId;
        this.me = me;
        this.outbound = outbound;
        this.progressTracker = progressTracker;
        this.sessionPool = sessionPool;
        this.progressTimeoutStrategy = progressTimeoutStrategy;
        this.availabilityTimeoutMillis = availabilityTimeoutMillis;
        this.log = logProvider.getLog( getClass() );
        this.databaseManager = databaseManager;
        this.replicationMonitor = monitors.newMonitor( ReplicationMonitor.class );
        this.leaderProvider = new LeaderProvider( leaderAwaitDuration );
        leaderLocator.registerListener( this );
    }

    @Override
    public Result replicate( ReplicatedContent command ) throws ReplicationFailureException
    {
        try
        {
            return replicate0( command );
        }
        catch ( Exception e )
        {
            if ( e instanceof InterruptedException )
            {
                Thread.currentThread().interrupt();
            }
            throw new ReplicationFailureException( "Failure during replication", e );
        }
    }

    private Result replicate0( ReplicatedContent command ) throws NoLeaderFoundException, InterruptedException, UnavailableException
    {
        // Awaiting the leader early allows us to avoid eating through local sessions unnecessarily.
        MemberId leader = leaderProvider.awaitLeaderOrThrow();

        replicationMonitor.startReplication();
        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        Progress progress = progressTracker.start( operation );

        Timeout progressTimeout = progressTimeoutStrategy.newTimeout();

        Result result = null;
        ReplicationLogger logger = new ReplicationLogger( log );
        try
        {
            do
            {
                logger.newAttempt( operation, leader );
                if ( tryReplicate( command, leader, operation, progress, progressTimeout ) )
                {
                    // We can only release a session which successfully replicated.
                    sessionPool.releaseSession( session );
                    replicationMonitor.successfulReplication();
                    logger.success( operation );

                    // Here we are awaiting the outcome of the command application, which will be registered in the progress tracker.
                    progress.awaitResult();
                    result = progress.result();
                }
                else
                {
                    // Refreshing the leader, in case a leader switch is the reason we failed to replicate!
                    leader = leaderProvider.awaitLeaderOrThrow();
                }
            }
            while ( result == null );
        }
        catch ( Throwable t )
        {
            progressTracker.abort( operation );
            replicationMonitor.failedReplication( t );
            throw t;
        }
        return result;
    }

    /**
     * Replication success means that the new entry was accepted by the leader and committed into the distributed raft log.
     *
     * @return true if the replication was successful, otherwise false.
     */
    private boolean tryReplicate( ReplicatedContent command, MemberId leader, DistributedOperation operation, Progress progress, Timeout replicationTimeout )
            throws UnavailableException, InterruptedException
    {
        replicationMonitor.replicationAttempt();

        assertDatabaseAvailable();

        // blocking at least until the send has succeeded or failed before retrying
        outbound.send( leader, new RaftMessages.NewEntry.Request( me, operation ), true );

        progress.awaitReplication( replicationTimeout.getAndIncrement() );
        return progress.isReplicated();
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
        var database = databaseManager.getDatabaseContext( databaseId )
                .map( DatabaseContext::database )
                .orElseThrow( IllegalStateException::new );

        database.getDatabaseAvailabilityGuard().await( availabilityTimeoutMillis );

        database.getDatabaseHealth().assertHealthy( IllegalStateException.class );
    }
}
