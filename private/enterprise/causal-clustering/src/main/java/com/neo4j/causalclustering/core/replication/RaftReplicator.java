/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.replication.session.OperationContext;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.time.Duration;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy.Timeout;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

/**
 * A replicator implementation suitable in a RAFT context. Will handle resending due to timeouts and leader switches.
 */
public class RaftReplicator implements Replicator, LeaderListener
{
    private final NamedDatabaseId namedDatabaseId;
    private final RaftMemberId me;
    private final Outbound<RaftMemberId,RaftMessage> outbound;
    private final ProgressTracker progressTracker;
    private final LocalSessionPool sessionPool;
    private final TimeoutStrategy progressTimeoutStrategy;
    private final Log log;
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final ReplicationMonitor replicationMonitor;
    private final long availabilityTimeoutMillis;
    private final LeaderProvider leaderProvider;

    // TODO: Get rid of dependency on database manager!
    public RaftReplicator( NamedDatabaseId namedDatabaseId, LeaderLocator leaderLocator, RaftMemberId me, Outbound<RaftMemberId,RaftMessage> outbound,
                           LocalSessionPool sessionPool, ProgressTracker progressTracker, TimeoutStrategy progressTimeoutStrategy,
                           long availabilityTimeoutMillis,
                           LogProvider logProvider, DatabaseManager<ClusteredDatabaseContext> databaseManager, Monitors monitors, Duration leaderAwaitDuration )
    {
        this.namedDatabaseId = namedDatabaseId;
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
    public ReplicationResult replicate( ReplicatedContent command )
    {
        replicationMonitor.clientRequest();
        try
        {
            assertDatabaseAvailable();
        }
        catch ( UnavailableException  | DatabaseNotFoundException e )
        {
            replicationMonitor.notReplicated();
            return ReplicationResult.notReplicated( e );
        }

        // Awaiting the leader early allows us to avoid eating through local sessions unnecessarily.
        RaftMemberId leader;

        try
        {
            leader = leaderProvider.awaitLeader();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            replicationMonitor.notReplicated();
            return ReplicationResult.notReplicated( e );
        }
        if ( leader == null )
        {
            replicationMonitor.notReplicated();
            return ReplicationResult.notReplicated( new IllegalStateException( "No leader found" ) );
        }

        OperationContext session = sessionPool.acquireSession();

        DistributedOperation operation = new DistributedOperation( command, session.globalSession(), session.localOperationId() );
        StateMachineResult stateMachineResult = null;
        Progress progress = progressTracker.start( operation );
        try
        {
            Timeout progressTimeout = progressTimeoutStrategy.newTimeout();
            ReplicationLogger logger = new ReplicationLogger( log );

            do
            {
                logger.newAttempt( operation, leader );
                if ( tryReplicate( leader, operation, progress, progressTimeout ) )
                {
                    // We can only release a session which successfully replicated.
                    sessionPool.releaseSession( session );
                    replicationMonitor.successfullyReplicated();
                    logger.success( operation );

                    // Here we are awaiting the outcome of the command application, which will be registered in the progress tracker.
                    progress.awaitResult();
                    stateMachineResult = progress.result();
                }
                else
                {
                    assertDatabaseAvailable();
                    // Refreshing the leader, in case a leader switch is the reason we failed to replicate!
                    leader = leaderProvider.awaitLeader();
                    if ( leader == null )
                    {
                        throw new IllegalStateException( "No Leader Found" );
                    }
                }
            }
            while ( stateMachineResult == null );
        }
        catch ( Throwable t )
        {
            if ( t instanceof InterruptedException )
            {
                Thread.currentThread().interrupt();
            }
            progressTracker.abort( operation );
            replicationMonitor.maybeReplicated();
            return ReplicationResult.maybeReplicated( t );
        }
        return ReplicationResult.applied( stateMachineResult );
    }

    /**
     * Replication success means that the new entry was accepted by the leader and committed into the distributed raft log.
     *
     * @return true if the replication was successful, otherwise false.
     */
    private boolean tryReplicate( RaftMemberId leader, DistributedOperation operation, Progress progress, Timeout replicationTimeout )
            throws InterruptedException
    {
        replicationMonitor.replicationAttempt();

        // blocking at least until the send has succeeded or failed before retrying
        outbound.send( leader, new RaftMessages.NewEntry.Request( me, operation ), true );

        progress.awaitReplication( replicationTimeout.getAndIncrement() );
        return progress.isReplicated();
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        progressTracker.triggerReplicationEvent();
        RaftMemberId newLeader = leaderInfo.memberId();
        RaftMemberId oldLeader = leaderProvider.currentLeader();
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
        var database = databaseManager.getDatabaseContext( namedDatabaseId )
                                      .map( DatabaseContext::database )
                                      .orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + namedDatabaseId.name() ) );

        database.getDatabaseAvailabilityGuard().await( availabilityTimeoutMillis );
        if ( !database.getDatabaseHealth().isHealthy() )
        {
            throw new UnavailableException( "Database is not healthy." );
        }
    }
}
