/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ReplicatedTransactionStateMachineTest
{
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final CommandIndexTracker commandIndexTracker = new CommandIndexTracker();
    private final int batchSize = 16;

    @Test
    public void shouldCommitTransaction() throws Exception
    {
        // given
        int lockSessionId = 23;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( lockSessionId ), DEFAULT_DATABASE_NAME );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        PageCursorTracer cursorTracer = mock( PageCursorTracer.class );

        ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine(
                commandIndexTracker, lockState( lockSessionId ), batchSize, logProvider, () -> cursorTracer,
                EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        // when
        stateMachine.applyCommand( tx, 0, r -> {} );
        stateMachine.ensuredApplied();

        // then
        verify( localCommitProcess ).commit( any( TransactionToApply.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) );
        verify( cursorTracer ).reportEvents();
    }

    @Test
    public void shouldFailFutureForTransactionCommittedUnderWrongLockSession()
    {
        // given
        int txLockSessionId = 23;
        int currentLockSessionId = 24;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( txLockSessionId ), DEFAULT_DATABASE_NAME );

        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );

        final ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( commandIndexTracker, lockState( currentLockSessionId ),
                        batchSize, logProvider,
                        PageCursorTracerSupplier.NULL, EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();
        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            try
            {
                result.consume();
                fail( "should have thrown" );
            }
            catch ( TransactionFailureException tfe )
            {
                assertEquals( Status.Transaction.LockSessionExpired, tfe.status() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    @Test
    public void shouldAcceptTransactionCommittedWithNoLockManager() throws Exception
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        int currentLockSessionId = 24;
        long txId = 42L;

        ReplicatedTransaction tx = ReplicatedTransaction.from( physicalTx( txLockSessionId ), DEFAULT_DATABASE_NAME );

        TransactionCommitProcess localCommitProcess = createFakeTransactionCommitProcess( txId );

        ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( commandIndexTracker, lockState( currentLockSessionId ), batchSize, logProvider,
                        PageCursorTracerSupplier.NULL, EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        AtomicBoolean called = new AtomicBoolean();

        // when
        stateMachine.applyCommand( tx, 0, result ->
        {
            // then
            called.set( true );
            try
            {
                assertEquals( txId, (long) result.consume() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );
        stateMachine.ensuredApplied();

        assertTrue( called.get() );
    }

    @Test
    public void raftIndexIsRecorded() throws TransactionFailureException
    {
        // given
        int txLockSessionId = Locks.Client.NO_LOCK_SESSION_ID;
        long anyTransactionId = 1234;
        long lastCommittedIndex = 1357;
        long updatedCommandIndex = 2468;

        // and
        ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( commandIndexTracker, lockState( txLockSessionId ), batchSize, logProvider, PageCursorTracerSupplier.NULL,
                        EmptyVersionContextSupplier.EMPTY );

        ReplicatedTransaction replicatedTransaction = ReplicatedTransaction.from( physicalTx( txLockSessionId ), DEFAULT_DATABASE_NAME );

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

    private TransactionCommitProcess createFakeTransactionCommitProcess( long txId ) throws TransactionFailureException
    {
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        when( localCommitProcess.commit(
                any( TransactionToApply.class), any( CommitEvent.class ), any( TransactionApplicationMode.class ) )
        ).thenAnswer( invocation ->
        {
            TransactionToApply txToApply = invocation.getArgument( 0 );
            txToApply.commitment( new FakeCommitment( txId, mock( TransactionIdStore.class ) ), txId );
            txToApply.commitment().publishAsCommitted();
            txToApply.commitment().publishAsClosed();
            txToApply.close();
            return txId;
        } );
        return localCommitProcess;
    }

    private PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLockSessionId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    private  ReplicatedLockTokenStateMachine lockState( int lockSessionId )
    {
        ReplicatedLockTokenRequest lockTokenRequest = new ReplicatedLockTokenRequest( null, lockSessionId, DEFAULT_DATABASE_NAME );
        ReplicatedLockTokenStateMachine lockState = mock( ReplicatedLockTokenStateMachine.class );
        when( lockState.snapshot() ).thenReturn( new ReplicatedLockTokenState( -1, lockTokenRequest ) );
        return lockState;
    }
}
