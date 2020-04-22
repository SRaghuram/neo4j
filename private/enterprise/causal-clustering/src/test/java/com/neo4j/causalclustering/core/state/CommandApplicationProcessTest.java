/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.core.CoreState;
import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;

class CommandApplicationProcessTest
{
    private final InMemoryRaftLog raftLog = spy( new InMemoryRaftLog() );

    private final SessionTracker sessionTracker = new SessionTracker(
            new InMemoryStateStorage<>( new GlobalSessionTrackerState() ) );

    private final GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), null );
    private final int flushEvery = 10;
    private final int batchSize = 16;

    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase().databaseId();

    private final InFlightCache inFlightCache = spy( new ConsecutiveInFlightCache() );
    private final Monitors monitors = new Monitors();
    private final CoreState coreState = mock( CoreState.class );
    private final SinglePanic panicker = new SinglePanic();
    private JobScheduler jobScheduler = new ThreadPoolJobScheduler();
    private final CommandApplicationProcess applicationProcess =
            new CommandApplicationProcess( raftLog, batchSize, flushEvery, NullLogProvider.getInstance(), new ProgressTrackerImpl( globalSession ),
                                           sessionTracker, coreState, inFlightCache, monitors, panicker, jobScheduler );

    private final ReplicatedTransaction nullTx = ReplicatedTransaction.from( new byte[0], databaseId );

    private final CommandDispatcher commandDispatcher = mock( CommandDispatcher.class );
    private int sequenceNumber;

    @BeforeEach
    void setUp()
    {
        when( coreState.commandDispatcher() ).thenReturn( commandDispatcher );
        when( coreState.getLastAppliedIndex() ).thenReturn( -1L );
        when( coreState.getLastFlushed() ).thenReturn( -1L );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        applicationProcess.stop();
        jobScheduler.shutdown();
    }

    @Test
    void shouldApplyCommittedCommand() throws Throwable
    {
        // given
        RaftLogCommitIndexMonitor listener = mock( RaftLogCommitIndexMonitor.class );
        monitors.addMonitorListener( listener );

        InOrder inOrder = inOrder( coreState, commandDispatcher );

        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 0L ), any() );
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), any() );
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 2L ), any() );
        inOrder.verify( commandDispatcher ).close();

        verify( listener ).commitIndex( 2 );
    }

    @Test
    void shouldNotApplyUncommittedCommands() throws Throwable
    {
        // given
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( -1 );
        applicationProcess.start();

        // then
        verifyNoInteractions( commandDispatcher );
    }

    @Test
    void entriesThatAreNotStateMachineCommandsShouldStillIncreaseCommandIndex() throws Throwable
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 1 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), any() );
        inOrder.verify( commandDispatcher ).close();
    }

    @Test
    void duplicatesShouldBeIgnoredButStillIncreaseCommandIndex() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 0 ) ) ) );
        // duplicate
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( nullTx, globalSession, new LocalOperationId( 0, 1 ) ) ) );

        // when
        applicationProcess.notifyCommitted( 3 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 1L ), any() );
        // duplicate not dispatched
        inOrder.verify( commandDispatcher ).dispatch( eq( nullTx ), eq( 3L ), any() );
        inOrder.verify( commandDispatcher ).close();
        verifyNoMoreInteractions( commandDispatcher );
    }

    @Test
    void outOfOrderDuplicatesShouldBeIgnoredButStillIncreaseCommandIndex() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 100 ), globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 101 ), globalSession, new LocalOperationId( 0, 1 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 102 ), globalSession, new LocalOperationId( 0, 2 ) ) ) );
        // duplicate of tx 101
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 101 ), globalSession, new LocalOperationId( 0, 1 ) ) ) );
        // duplicate of tx 100
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 100 ), globalSession, new LocalOperationId( 0, 0 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 103 ), globalSession, new LocalOperationId( 0, 3 ) ) ) );
        raftLog.append( new RaftLogEntry( 0, new DistributedOperation( tx( (byte) 104 ), globalSession, new LocalOperationId( 0, 4 ) ) ) );

        // when
        applicationProcess.notifyCommitted( 6 );
        applicationProcess.start();

        // then
        InOrder inOrder = inOrder( coreState, commandDispatcher );
        inOrder.verify( coreState ).commandDispatcher();
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 100 ) ), eq( 0L ), any() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 101 ) ), eq( 1L ), any() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 102 ) ), eq( 2L ), any() );
        // duplicate of tx 101 not dispatched, at index 3
        // duplicate of tx 100 not dispatched, at index 4
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 103 ) ), eq( 5L ), any() );
        inOrder.verify( commandDispatcher ).dispatch( eq( tx( (byte) 104 ) ), eq( 6L ), any() );
        inOrder.verify( commandDispatcher ).close();
        verifyNoMoreInteractions( commandDispatcher );
    }

    // TODO: Test recovery, see CoreState#start().

    @Test
    void shouldPeriodicallyFlushState() throws Throwable
    {
        // given
        int interactions = flushEvery * 5;
        for ( int i = 0; i < interactions; i++ )
        {
            raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        }

        // when
        applicationProcess.notifyCommitted( raftLog.appendIndex() );
        applicationProcess.start();

        // then
        verify( coreState ).flush( batchSize - 1 );
        verify( coreState ).flush( 2 * batchSize - 1 );
        verify( coreState ).flush( 3 * batchSize - 1 );
    }

    @Test
    void shouldPanicIfUnableToApply() throws Throwable
    {
        // given
        doThrow( RuntimeException.class ).when( commandDispatcher )
                .dispatch( any( ReplicatedTransaction.class ), anyLong(), any() );
        applicationProcess.start();

        // when
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        applicationProcess.notifyCommitted( 0 );

        assertEventually( "failed apply", () -> panicker.hasPanicked, value -> value, 5, SECONDS );
    }

    @Test
    void shouldApplyToLogFromCache() throws Throwable
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 1, operation( nullTx ) ) );

        //when
        applicationProcess.notifyCommitted( 0 );
        applicationProcess.start();

        //then the cache should have had it's get method called.
        verify( inFlightCache ).get( 0L );
        verifyNoInteractions( raftLog );
    }

    @Test
    void cacheEntryShouldBePurgedAfterBeingApplied() throws Throwable
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightCache.put( 1L, new RaftLogEntry( 0, operation( nullTx ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 0 );
        applicationProcess.start();

        // then the cache should have had its get method called.
        assertNull( inFlightCache.get( 0L ) );
        assertNotNull( inFlightCache.get( 1L ) );
        assertNotNull( inFlightCache.get( 2L ) );
    }

    @Test
    void shouldFailWhenCacheAndLogMiss()
    {
        // given
        inFlightCache.put( 0L, new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 1, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        assertThrows( IllegalStateException.class, applicationProcess::start );
    }

    @Test
    void shouldIncreaseLastAppliedForStateMachineCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        assertEquals( 2, applicationProcess.lastApplied() );
    }

    @Test
    void shouldIncreaseLastAppliedForOtherCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 0, new NewLeaderBarrier() ) );

        // when
        applicationProcess.notifyCommitted( 2 );
        applicationProcess.start();

        // then
        assertEquals( 2, applicationProcess.lastApplied() );
    }

    private ReplicatedTransaction tx( byte dataValue )
    {
        byte[] dataArray = new byte[30];
        Arrays.fill( dataArray, dataValue );
        return ReplicatedTransaction.from( dataArray, databaseId );
    }

    private synchronized ReplicatedContent operation( CoreReplicatedContent tx )
    {
        return new DistributedOperation( tx, globalSession, new LocalOperationId( 0, sequenceNumber++ ) );
    }

    private static class SinglePanic implements DatabasePanicker
    {
        volatile boolean hasPanicked;

        @Override
        public void panic( Throwable e )
        {
            hasPanicked = true;
        }
    }
}
