/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;

class StateMachineCommitHelperTest
{
    private final CommandIndexTracker commandIndexTracker = new CommandIndexTracker();
    private final VersionContextSupplier versionContextSupplier = EmptyVersionContextSupplier.EMPTY;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch = mock( ReplicatedDatabaseEventDispatch.class );

    private final StateMachineCommitHelper commitHelper = new StateMachineCommitHelper( commandIndexTracker, versionContextSupplier,
            databaseEventDispatch, PageCacheTracer.NULL );

    @Test
    void shouldUpdateLastAppliedCommandIndex()
    {
        assertEquals( 0, commandIndexTracker.getAppliedCommandIndex() );

        commitHelper.updateLastAppliedCommandIndex( 42 );

        assertEquals( 42, commandIndexTracker.getAppliedCommandIndex() );
    }

    @Test
    void shouldCommitTransaction() throws Exception
    {
        var commitProcess = newCommitProcessMock( 1 );
        var tx = new TransactionToApply( new PhysicalTransactionRepresentation( emptyList() ), PageCursorTracer.NULL );

        commitHelper.commit( commitProcess, tx );

        verify( commitProcess ).commit( tx, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
    }

    @Test
    void shouldCommitTransactionRepresentation() throws Exception
    {
        var commitProcess = newCommitProcessMock( 2 );
        var tx = new PhysicalTransactionRepresentation( emptyList() );

        commitHelper.commit( commitProcess, tx, 42 );

        var txToApplyCaptor = ArgumentCaptor.forClass( TransactionToApply.class );
        verify( commitProcess ).commit( txToApplyCaptor.capture(), eq( CommitEvent.NULL ), eq( TransactionApplicationMode.EXTERNAL ) );
        assertEquals( tx, txToApplyCaptor.getValue().transactionRepresentation() );
    }

    @Test
    void shouldBuildTransactionAndCommit() throws Exception
    {
        var txId = 99;
        var commitProcess = newCommitProcessMock( txId );

        var committedTxId = new MutableLong();
        var tx = commitHelper.newTransactionToApply( new PhysicalTransactionRepresentation( emptyList() ), 15, committedTxId::setValue );

        commitHelper.commit( commitProcess, tx );

        verify( commitProcess ).commit( tx, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
        assertEquals( txId, committedTxId.longValue() );
        assertEquals( 15, commandIndexTracker.getAppliedCommandIndex() );
        verify( databaseEventDispatch ).fireTransactionCommitted( txId );
    }

    private static TransactionCommitProcess newCommitProcessMock( long transactionId ) throws TransactionFailureException
    {
        var commitProcess = mock( TransactionCommitProcess.class );

        doAnswer( invocation ->
        {
            var tx = invocation.getArgument( 0, TransactionToApply.class );
            tx.commitment( NO_COMMITMENT, transactionId );
            tx.close();
            return null;
        } ).when( commitProcess ).commit( any( TransactionToApply.class ), eq( CommitEvent.NULL ), eq( TransactionApplicationMode.EXTERNAL ) );

        return commitProcess;
    }
}
