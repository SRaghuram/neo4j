/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.replication.monitoring.ReplicationMonitor;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

class RaftReplicatorTest
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;

    private LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private LeaderInfo leaderInfo = new LeaderInfo( new MemberId( UUID.randomUUID() ), 1 );
    private GlobalSession session = new GlobalSession( UUID.randomUUID(), myself );
    private LocalSessionPool sessionPool = new LocalSessionPool( session );
    private TimeoutStrategy noWaitTimeoutStrategy = new ConstantTimeTimeoutStrategy( 0, MILLISECONDS );
    private DatabaseAvailabilityGuard availabilityGuard;
    private DatabaseService databaseService;

    @BeforeEach
    void setUp()
    {
        availabilityGuard = new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, Clocks.systemClock(), NullLog.getInstance() );
        databaseService = mock( DatabaseService.class );
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

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, times( 1 ) ).successfulReplication();
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

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 2 ) ).replicationAttempt();
        verify( replicationMonitor, times( 1 ) ).successfulReplication();
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

        verify( replicationMonitor, times( 1 ) ).startReplication();
        verify( replicationMonitor, atLeast( 1 ) ).replicationAttempt();
        verify( replicationMonitor, never() ).successfulReplication();
        verify( replicationMonitor, times( 1 ) ).failedReplication( any() );
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
        CapturingProgressTracker capturedProgress = new CapturingProgressTracker();
        CapturingOutbound<RaftMessages.RaftMessage> outbound = new CapturingOutbound<>();
        doThrow( new ReplicationFailureException( "" ) ).when( databaseService ).assertHealthy( IllegalStateException.class );

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
        return new RaftReplicator( leaderLocator, myself, outbound, sessionPool, progressTracker, noWaitTimeoutStrategy, 10, availabilityGuard,
                NullLogProvider.getInstance(), databaseService, monitors );
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
