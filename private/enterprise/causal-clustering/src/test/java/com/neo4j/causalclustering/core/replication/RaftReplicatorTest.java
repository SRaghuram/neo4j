/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.core.state.Result;
import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Message;
import com.neo4j.causalclustering.messaging.Outbound;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.UUID;

import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( LifeExtension.class )
class RaftReplicatorTest
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;

    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase();
    private final LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private final MemberId myself = new MemberId( UUID.randomUUID() );
    private final LeaderInfo leaderInfo = new LeaderInfo( new MemberId( UUID.randomUUID() ), 1 );
    private final GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private final LocalSessionPool sessionPool = new LocalSessionPool( session );
    private final TimeoutStrategy noWaitTimeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private final Duration leaderAwaitDuration = Duration.ofMillis( 500 );
    private final Health health = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
    private DatabaseAvailabilityGuard availabilityGuard;
    private StubClusteredDatabaseManager databaseManager;

    @Inject
    private LifeSupport lifeSupport;

    @BeforeEach
    void setUp()
    {
        availabilityGuard = new DatabaseAvailabilityGuard( databaseId, Clocks.systemClock(), NullLog.getInstance(), 0,
                mock( CompositeDatabaseAvailabilityGuard.class ) );

        databaseManager = new StubClusteredDatabaseManager();
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( databaseId )
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
        assertEventually( "making progress", () -> capturedProgress.last, not( equalTo( null ) ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( Result.of( 5 ) );

        // then
        replicatingThread.join( DEFAULT_TIMEOUT_MS );
        assertEquals( leaderInfo.memberId(), outbound.lastTo );

        verify( replicationMonitor ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor ).successfulReplication();
        verify( replicationMonitor, never() ).failedReplication( any() );
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
        assertEventually( "send count", () -> outbound.count, greaterThan( 2 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // cleanup
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( Result.of( 5 ) );
        replicatingThread.join( DEFAULT_TIMEOUT_MS );

        verify( replicationMonitor ).startReplication();
        verify( replicationMonitor, atLeast( 2 ) ).replicationAttempt();
        verify( replicationMonitor ).successfulReplication();
        verify( replicationMonitor, never() ).failedReplication( any() );
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
        assertEventually( "making progress", () -> capturedProgress.last, not( equalTo( null ) ),
                DEFAULT_TIMEOUT_MS, MILLISECONDS );
        assertEquals( 1, sessionPool.openSessionCount() );

        // when
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( Result.of( 5 ) );
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
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( UnavailableException.class ) );

        verify( replicationMonitor ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, never() ).successfulReplication();
        verify( replicationMonitor ).failedReplication( any() );
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
        assertThat( replicatingThread.getReplicationException().getCause(), Matchers.instanceOf( UnavailableException.class ) );
    }

    @Test
    void stopReplicationWhenUnHealthy() throws InterruptedException
    {
        health.panic( new ReplicationFailureException( "" ) );

        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();

        RaftReplicator replicator = getReplicator( outbound, capturedProgress, new Monitors() );
        replicator.onLeaderSwitch( leaderInfo );

        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );
        ReplicatingThread replicatingThread = replicatingThread( replicator, content );

        // when
        replicatingThread.start();

        replicatingThread.join();
        Assertions.assertNotNull( replicatingThread.getReplicationException() );
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
        assertThrows( ReplicationFailureException.class, () -> replicator.replicate( content ) );
    }

    @Test
    void shouldListenToLeaderUpdates() throws ReplicationFailureException
    {
        OneProgressTracker oneProgressTracker = new OneProgressTracker();
        oneProgressTracker.last.setReplicated();
        oneProgressTracker.last.registerResult( Result.of( null ) );
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();
        RaftReplicator replicator = getReplicator( outbound, oneProgressTracker, new Monitors() );
        ReplicatedInteger content = ReplicatedInteger.valueOf( 5 );

        LeaderInfo lastLeader = leaderInfo;

        // set initial leader, sens to that leader
        replicator.onLeaderSwitch( lastLeader );
        replicator.replicate( content );
        assertEquals( outbound.lastTo, lastLeader.memberId() );

        // update with valid new leader, sends to new leader
        lastLeader = new LeaderInfo( new MemberId( UUID.randomUUID() ), 1 );
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
        assertEventually( "send count", () -> outbound.count, greaterThan( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        replicator.onLeaderSwitch( new LeaderInfo( null, 1 ) );
        capturedProgress.last.setReplicated();
        capturedProgress.last.registerResult( Result.of( 5 ) );
        replicator.onLeaderSwitch( leaderInfo );

        replicatingThread.join( DEFAULT_TIMEOUT_MS );
    }

    private RaftReplicator getReplicator( CapturingOutbound<RaftMessages.RaftMessage> outbound, ProgressTracker progressTracker, Monitors monitors )
    {
        return new RaftReplicator( databaseId, leaderLocator, myself, outbound, sessionPool, progressTracker, noWaitTimeoutStrategy, 10,
                NullLogProvider.getInstance(), databaseManager, monitors, leaderAwaitDuration );
    }

    private ReplicatingThread replicatingThread( RaftReplicator replicator, ReplicatedInteger content )
    {
        return new ReplicatingThread( replicator, content );
    }

    private class ReplicatingThread extends Thread
    {

        private final RaftReplicator replicator;
        private final ReplicatedInteger content;
        private volatile Exception replicationException;

        ReplicatingThread( RaftReplicator replicator, ReplicatedInteger content )
        {
            this.replicator = replicator;
            this.content = content;
        }

        @Override
        public void run()
        {
            try
            {
                replicator.replicate( content ).consume();
            }
            catch ( Exception e )
            {
                replicationException = e;
            }
        }

        Exception getReplicationException()
        {
            return replicationException;
        }
    }

    private class OneProgressTracker extends ProgressTrackerAdaptor
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

    private class CapturingProgressTracker extends ProgressTrackerAdaptor
    {
        @Override
        public Progress start( DistributedOperation operation )
        {
            last = new Progress();
            return last;
        }
    }

    private abstract class ProgressTrackerAdaptor implements ProgressTracker
    {
        protected Progress last;

        @Override
        public void trackReplication( DistributedOperation operation )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void trackResult( DistributedOperation operation, Result result )
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

    private static class CapturingOutbound<MESSAGE extends Message> implements Outbound<MemberId, MESSAGE>
    {
        private MemberId lastTo;
        private int count;

        @Override
        public void send( MemberId to, MESSAGE message, boolean block )
        {
            this.lastTo = to;
            this.count++;
        }

    }
}
