/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.DummyStateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseStateMachine;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplicatedTransactionStateMachineTest
{
    private static final NamedDatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final CommandIndexTracker commandIndexTracker = new CommandIndexTracker();

    @Test
    void shouldCommitTransaction() throws Exception
    {
        // given
        int leaseId = 23;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( leaseId ), DATABASE_ID, LogEntryWriterFactory.LATEST );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        ReplicatedTransactionStateMachine stateMachine = newTransactionStateMachine( leaseState( leaseId ), PageCacheTracer.NULL );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        stateMachine.applyCommand( tx, 0, r -> {} );
        stateMachine.ensuredApplied();

        // then
        verify( localCommitProcess ).commit( any( TransactionToApply.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) );
    }

    @Test
    void shouldFailFutureForTransactionCommittedUnderWrongLease()
    {
        // given
        int txLeaseId = 23;
        int currentLeaseId = 24;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( txLeaseId ), DATABASE_ID, LogEntryWriterFactory.LATEST );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        ReplicatedTransactionStateMachine stateMachine = newTransactionStateMachine( leaseState( currentLeaseId ) );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();
        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, result::consume );
            assertEquals( Status.Transaction.LeaseExpired, exception.status() );
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    @Test
    void shouldAcceptTransactionCommittedWithNoLockManager() throws Exception
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        int currentLockSessionId = 24;
        long txId = 42L;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( txLockSessionId ), DATABASE_ID, LogEntryWriterFactory.LATEST );

        TransactionCommitProcess localCommitProcess = createFakeTransactionCommitProcess( txId );

        ReplicatedTransactionStateMachine stateMachine = newTransactionStateMachine( leaseState( currentLockSessionId ) );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();

        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            assertDoesNotThrow( () -> assertEquals( txId, (long) result.consume() ) );
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    @Test
    void raftIndexIsRecorded() throws TransactionFailureException
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        long anyTransactionId = 1234;
        long lastCommittedIndex = 1357;
        long updatedCommandIndex = 2468;

        // and
        ReplicatedTransactionStateMachine stateMachine = newTransactionStateMachine( leaseState( txLockSessionId ) );

        ReplicatedTransaction replicatedTransaction = ReplicatedTransaction.from( physicalTx( txLockSessionId ), DATABASE_ID, LogEntryWriterFactory.LATEST );

        // and
        TransactionCommitProcess localCommitProcess = createFakeTransactionCommitProcess( anyTransactionId );

        // when
        stateMachine.installCommitProcess( localCommitProcess, lastCommittedIndex );

        // then
        assertEquals( lastCommittedIndex, commandIndexTracker.getAppliedCommandIndex() );

        // when
        stateMachine.applyCommand( replicatedTransaction, updatedCommandIndex, result -> {});
        stateMachine.ensuredApplied();

        // then
        assertEquals( updatedCommandIndex, commandIndexTracker.getAppliedCommandIndex() );
    }

    private static TransactionCommitProcess createFakeTransactionCommitProcess( long txId ) throws TransactionFailureException
    {
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        when( localCommitProcess.commit(
                any( TransactionToApply.class), any( CommitEvent.class ), any( TransactionApplicationMode.class ) )
        ).thenAnswer( invocation ->
        {
            TransactionToApply txToApply = invocation.getArgument( 0 );
            txToApply.commitment( new FakeCommitment( txId, mock( TransactionIdStore.class ) ), txId );
            txToApply.publishAsCommitted();
            txToApply.publishAsClosed();
            txToApply.close();
            return txId;
        } );
        return localCommitProcess;
    }

    private static PhysicalTransactionRepresentation physicalTx( int leaseId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLeaseId() ).thenReturn( leaseId );
        return physicalTx;
    }

    private static ReplicatedLeaseStateMachine leaseState( int leaseId )
    {
        ReplicatedLeaseRequest leaseRequest = new ReplicatedLeaseRequest( null, leaseId, DATABASE_ID.databaseId() );
        ReplicatedLeaseStateMachine leaseState = mock( ReplicatedLeaseStateMachine.class );
        when( leaseState.snapshot() ).thenReturn( new ReplicatedLeaseState( -1, leaseRequest ) );
        return leaseState;
    }

    private ReplicatedTransactionStateMachine newTransactionStateMachine( ReplicatedLeaseStateMachine leaseState )
    {
        return newTransactionStateMachine( leaseState, PageCacheTracer.NULL );
    }

    private ReplicatedTransactionStateMachine newTransactionStateMachine( ReplicatedLeaseStateMachine lockState, PageCacheTracer pageCacheTracer )
    {
        var batchSize = 16;
        var commitHelper = new DummyStateMachineCommitHelper( commandIndexTracker, pageCacheTracer );
        return new ReplicatedTransactionStateMachine( commitHelper, lockState, batchSize, logProvider, new TestCommandReaderFactory() );
    }
}
