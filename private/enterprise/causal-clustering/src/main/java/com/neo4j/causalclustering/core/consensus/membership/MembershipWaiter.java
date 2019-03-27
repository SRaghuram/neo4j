/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Waits until member has "fully joined" the raft membership.
 * We consider a member fully joined where:
 * <ul>
 * <li>It is a member of the voting group
 * (its opinion will count towards leader elections and committing entries), and</li>
 * <li>It is sufficiently caught up with the leader,
 * so that long periods of unavailability are unlikely, should the leader fail.</li>
 * </ul>
 * <p>
 * To determine whether the member is sufficiently caught up, we check periodically how far behind we are,
 * once every {@code maxCatchupLag}. If the leader is always moving forwards we will never fully catch up,
 * so all we look for is that we have caught up with where the leader was the <i>previous</i> time
 * that we checked.
 */
public class MembershipWaiter
{
    public interface Monitor
    {
        void waitingToHearFromLeader();

        void waitingToCatchupWithLeader( long localCommitIndex, long leaderCommitIndex );

        void joinedRaftGroup();
    }

    private final MemberId myself;
    private final JobScheduler jobScheduler;
    private final DatabaseHealth internalDatabasesHealth;
    private final long maxCatchupLag;
    private long currentCatchupDelayInMs;
    private final Log log;
    private final Monitor monitor;

    public MembershipWaiter( MemberId myself, JobScheduler jobScheduler, DatabaseHealth internalDatabasesHealth,
            long maxCatchupLag, LogProvider logProvider, Monitors monitors )
    {
        this.myself = myself;
        this.jobScheduler = jobScheduler;
        this.internalDatabasesHealth = internalDatabasesHealth;
        this.maxCatchupLag = maxCatchupLag;
        this.currentCatchupDelayInMs = maxCatchupLag;
        this.log = logProvider.getLog( getClass() );
        this.monitor = monitors.newMonitor( Monitor.class );
    }

    CompletableFuture<Boolean> waitUntilCaughtUpMember( RaftMachine raft )
    {
        CompletableFuture<Boolean> catchUpFuture = new CompletableFuture<>();

        Evaluator evaluator = new Evaluator( raft, catchUpFuture, internalDatabasesHealth );

        JobHandle jobHandle = jobScheduler.schedule( Group.MEMBERSHIP_WAITER,
                evaluator, currentCatchupDelayInMs, MILLISECONDS );

        catchUpFuture.whenComplete( ( result, e ) -> jobHandle.cancel( true ) );

        return catchUpFuture;
    }

    private class Evaluator implements Runnable
    {
        private final RaftMachine raft;
        private final CompletableFuture<Boolean> catchUpFuture;

        private long lastLeaderCommit;
        private final DatabaseHealth kernelDatabaseHealth;

        private Evaluator( RaftMachine raft, CompletableFuture<Boolean> catchUpFuture, DatabaseHealth kernelDatabaseHealth )
        {
            this.raft = raft;
            this.catchUpFuture = catchUpFuture;
            this.lastLeaderCommit = raft.state().leaderCommit();
            this.kernelDatabaseHealth = kernelDatabaseHealth;
        }

        @Override
        public void run()
        {
            if ( !kernelDatabaseHealth.isHealthy() )
            {
                catchUpFuture.completeExceptionally( kernelDatabaseHealth.cause() );
            }
            else if ( iAmAVotingMember() && caughtUpWithLeader() )
            {
                catchUpFuture.complete( Boolean.TRUE );
                monitor.joinedRaftGroup();
            }
            else
            {
                currentCatchupDelayInMs += SECONDS.toMillis( 1 );
                long longerDelay = currentCatchupDelayInMs < maxCatchupLag ? currentCatchupDelayInMs : maxCatchupLag;
                jobScheduler.schedule( Group.MEMBERSHIP_WAITER, this,
                        longerDelay, MILLISECONDS );
            }
        }

        private boolean iAmAVotingMember()
        {
            Set votingMembers = raft.state().votingMembers();
            boolean votingMember = votingMembers.contains( myself );
            if ( !votingMember )
            {
                log.debug( "I (%s) am not a voting member: [%s]", myself, votingMembers );
            }
            return votingMember;
        }

        private boolean caughtUpWithLeader()
        {
            boolean caughtUpWithLeader = false;

            ExposedRaftState state = raft.state();
            long localCommit = state.commitIndex();
            lastLeaderCommit = state.leaderCommit();
            if ( lastLeaderCommit != -1 )
            {
                caughtUpWithLeader = localCommit == lastLeaderCommit;
                monitor.waitingToCatchupWithLeader( localCommit, lastLeaderCommit );
            }
            else
            {
                monitor.waitingToHearFromLeader();
            }
            return caughtUpWithLeader;
        }
    }

}
