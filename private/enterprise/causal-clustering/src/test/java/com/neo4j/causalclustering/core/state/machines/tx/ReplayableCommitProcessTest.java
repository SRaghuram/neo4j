/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class ReplayableCommitProcessTest
{
    @Test
    void shouldCommitTransactions() throws Exception
    {
        // given
        TransactionToApply newTx1 = mock( TransactionToApply.class );
        TransactionToApply newTx2 = mock( TransactionToApply.class );
        TransactionToApply newTx3 = mock( TransactionToApply.class );

        StubLocalDatabase localDatabase = new StubLocalDatabase( 1 );
        final ReplayableCommitProcess txListener = new ReplayableCommitProcess(
                localDatabase, localDatabase );

        // when
        txListener.commit( newTx1, NULL, EXTERNAL );
        txListener.commit( newTx2, NULL, EXTERNAL );
        txListener.commit( newTx3, NULL, EXTERNAL );

        // then
        verify( localDatabase.commitProcess, times( 3 ) ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    void shouldNotCommitTransactionsThatAreAlreadyCommittedLocally() throws Exception
    {
        // given
        TransactionToApply alreadyCommittedTx1 = mock( TransactionToApply.class );
        TransactionToApply alreadyCommittedTx2 = mock( TransactionToApply.class );
        TransactionToApply newTx = mock( TransactionToApply.class );

        StubLocalDatabase localDatabase = new StubLocalDatabase( 3 );
        final ReplayableCommitProcess txListener = new ReplayableCommitProcess(
                localDatabase, localDatabase );

        // when
        txListener.commit( alreadyCommittedTx1, NULL, EXTERNAL );
        txListener.commit( alreadyCommittedTx2, NULL, EXTERNAL );
        txListener.commit( newTx, NULL, EXTERNAL );

        // then
        verify( localDatabase.commitProcess ).commit( eq( newTx ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
        verifyNoMoreInteractions( localDatabase.commitProcess );
    }

    private static class StubLocalDatabase extends LifecycleAdapter implements TransactionCounter, TransactionCommitProcess
    {
        long lastCommittedTransactionId;
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        StubLocalDatabase( long lastCommittedTransactionId )
        {
            this.lastCommittedTransactionId = lastCommittedTransactionId;
        }

        @Override
        public long lastCommittedTransactionId()
        {
            return lastCommittedTransactionId;
        }

        @Override
        public long commit( TransactionToApply tx, CommitEvent commitEvent,
                            TransactionApplicationMode mode ) throws TransactionFailureException
        {
            lastCommittedTransactionId++;
            return commitProcess.commit( tx, commitEvent, mode );
        }
    }
}
