/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import org.assertj.core.api.Condition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.MAYBE_REPLICATED;
import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.NOT_REPLICATED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.monitoring.PanicEventGenerator.NO_OP;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( LifeExtension.class )
class RaftReplicatorTest
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;
    private static final Condition<Progress> NOT_NULL_CONDITION = new Condition<>( Objects::nonNull, "Should be not null." );

    private final NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
    private final LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private final RaftMemberId myself = IdFactory.randomRaftMemberId();
    private final LeaderInfo leaderInfo = new LeaderInfo( IdFactory.randomRaftMemberId(), 1 );
    private final GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private final LocalSessionPool sessionPool = new LocalSessionPool( session );
    private final TimeoutStrategy noWaitTimeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private final Duration leaderAwaitDuration = Duration.ofMillis( 500 );
    private final DatabaseHealth health = new DatabaseHealth( NO_OP, NullLog.getInstance() );
    private DatabaseAvailabilityGuard availabilityGuard;
    private StubClusteredDatabaseManager databaseManager;

    @Inject
    private LifeSupport lifeSupport;

    @BeforeEach
    void setUp()
    {
        availabilityGuard = new DatabaseAvailabilityGuard( namedDatabaseId, Clocks.systemClock(), NullLog.getInstance(), 0,
                mock( CompositeDatabaseAvailabilityGuard.class ) );

        databaseManager = new StubClusteredDatabaseManager();
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( namedDatabaseId )
                       .withDatabaseAvailabilityGuard( availabilityGuard )
                       .withDatabaseHealth( health )
                       .register();

        lifeSupport.add( availabilityGuard );
    }

    @Test
    void shouldSendReplicatedContentToLeader() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();
        // then
        assertEventually( "making progress", () -> capturedProgress.last, NOT_NULL_CONDITION, DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( StateMachineResult.of( 5 ) );

        // then
        replicatingThread.join( DEFAULT_TIMEOUT_MS );
        assertEquals( leaderInfo.memberId(), outbound.lastTo );

        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor ).successfullyReplicated();
    }

    @Test
    void shouldResendAfterTimeout() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();
        // then
        assertEventually( "send count", () -> outbound.count, value -> value > 2, DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // cleanup
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( StateMachineResult.of( 5 ) );
        replicatingThread.join( DEFAULT_TIMEOUT_MS );

        verify( replicationMonitor, atLeast( 2 ) ).replicationAttempt();
        verify( replicationMonitor ).successfullyReplicated();
    }

    @Test
    void shouldReleaseSessionWhenFinished() throws Exception
    {
        // given
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        Thread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();

        // then
        assertEventually( "making progress", () -> capturedProgress.last, NOT_NULL_CONDITION, DEFAULT_TIMEOUT_MS, MILLISECONDS );
        assertEquals( 1, sessionPool.openSessionCount() );

        // when
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( StateMachineResult.of( 5 ) );
        replicatingThread.join( DEFAULT_TIMEOUT_MS );

        // then
        assertEquals( 0, sessionPool.openSessionCount() );
    }

    @Test
    void stopReplicationOnShutdown() throws InterruptedException
    {
        // given
        Monitors monitors = new Monitors();
        ReplicationMonitor replicationMonitor = mock( ReplicationMonitor.class );
        monitors.addMonitorListener( replicationMonitor );
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, monitors );
        replicator.onLeaderSwitch( leaderInfo );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();
        availabilityGuard.shutdown();
        replicatingThread.join();

        ReplicationResult replicationResult = replicatingThread.getReplicationResult();
        assertThat( replicationResult.outcome(), either( equalTo( NOT_REPLICATED ) ).or( equalTo( MAYBE_REPLICATED ) ) );
        assertThat( replicationResult.failure(), Matchers.instanceOf( UnavailableException.class ) );

        if ( replicationResult.outcome() == NOT_REPLICATED )
        {
            verify( replicationMonitor ).notReplicated();
        }
        else
        {
            verify( replicationMonitor ).maybeReplicated();
        }
    }

    @Test
    void stopReplicationWhenUnavailable() throws InterruptedException
    {
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();
        availabilityGuard.require( () -> "Database not unavailable" );
        replicatingThread.join();

        ReplicationResult replicationResult = replicatingThread.getReplicationResult();
        assertThat( replicationResult.outcome(), either( equalTo( NOT_REPLICATED ) ).or( equalTo( MAYBE_REPLICATED ) ) );
        assertThat( replicationResult.failure(), Matchers.instanceOf( UnavailableException.class ) );
    }

    @Test
    void stopReplicationWhenUnHealthy() throws InterruptedException
    {
        health.panic( new RuntimeException( "" ) );

        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();

        replicatingThread.join();
        Assertions.assertNotNull( replicatingThread.getReplicationResult() );
    }

    @Test
    void shouldFailIfNoLeaderIsAvailable()
    {
        // given
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );

        // when
        ReplicationResult replicationResult = replicator.replicate( content );
        assertEquals( NOT_REPLICATED, replicationResult.outcome() );
    }

    @Test
    void shouldListenToLeaderUpdates()
    {
        OneProgressTracker oneProgressTracker = new OneProgressTracker();
        oneProgressTracker.last.setReplicated();
        oneProgressTracker.last.registerResult( StateMachineResult.of( null ) );
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();
        RaftReplicator replicator = getReplicator( outbound, oneProgressTracker, new Monitors() );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );

        LeaderInfo lastLeader = leaderInfo;

        // set initial leader, sens to that leader
        replicator.onLeaderSwitch( lastLeader );
        replicator.replicate( content );
        assertEquals( outbound.lastTo, lastLeader.memberId() );

        // update with valid new leader, sends to new leader
        lastLeader = new LeaderInfo( IdFactory.randomRaftMemberId(), 1 );
        replicator.onLeaderSwitch( lastLeader );
        replicator.replicate( content );
        assertEquals( outbound.lastTo, lastLeader.memberId() );
    }

    @Test
    void shouldSuccessfullySendIfLeaderIsLostAndFound() throws InterruptedException
    {
        OneProgressTracker capturedProgress = new OneProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();

        // then
        assertEventually( "send count", () -> outbound.count, value -> value > 1, DEFAULT_TIMEOUT_MS, MILLISECONDS );
        replicator.onLeaderSwitch( new LeaderInfo( null, 1 ) );
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( StateMachineResult.of( 5 ) );
        replicator.onLeaderSwitch( leaderInfo );

        replicatingThread.join( DEFAULT_TIMEOUT_MS );
    }

    private RaftReplicator getReplicator( CapturingOutbound<RaftMessages.RaftMessage> outbound, ProgressTracker progressTracker, Monitors monitors )
    {
        return new RaftReplicator( namedDatabaseId, leaderLocator, myself, outbound, sessionPool, progressTracker, noWaitTimeoutStrategy, 10,
                NullLogProvider.getInstance(), databaseManager, monitors, leaderAwaitDuration );
    }

    private ReplicatingThread replicatingThread( RaftReplicator replicator, ReplicatedInteger content )
    {
        return new ReplicatingThread( replicator, content );
    }

    private static class ReplicatingThread extends Thread
    {
        private final RaftReplicator replicator;
        private final ReplicatedInteger content;
        private volatile ReplicationResult replicationResult;

        ReplicatingThread( RaftReplicator replicator, ReplicatedInteger content )
        {
            this.replicator = replicator;
            this.content = content;
        }

        @Override
        public void run()
        {
            replicationResult = replicator.replicate( content );
        }

        ReplicationResult getReplicationResult()
        {
            return replicationResult;
        }
    }

    private static class OneProgressTracker extends ProgressTrackerAdaptor
    {
        OneProgressTracker()
        {
            last = new Progress();
        }

        @Override
        public Progress start( DistributedOperation operation )
        {
            return last;
        }
    }

    private static class CapturingProgressTracker extends ProgressTrackerAdaptor
    {
        @Override
        public Progress start( DistributedOperation operation )
        {
            last = new Progress();
            return last;
        }
    }

    private abstract static class ProgressTrackerAdaptor implements ProgressTracker
    {
        protected Progress last;

        @Override
        public void trackReplication( DistributedOperation operation )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void trackResult( DistributedOperation operation, StateMachineResult result )
        {
            last.registerResult( result );
        }

        @Override
        public void abort( DistributedOperation operation )
        {
            // do nothing
        }

        @Override
        public void triggerReplicationEvent()
        {
            // do nothing
        }

        @Override
        public int inProgressCount()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CapturingOutbound<MESSAGE> implements Outbound<RaftMemberId,MESSAGE>
    {
        private RaftMemberId lastTo;
        private int count;

        @Override
        public void send( RaftMemberId to, MESSAGE message, boolean block )
        {
            this.lastTo = to;
            this.count++;
        }
    }
}
