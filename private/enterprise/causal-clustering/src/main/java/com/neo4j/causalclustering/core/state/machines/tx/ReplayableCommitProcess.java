/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

/**
 * Counts transactions, and only applies new transactions once it has already seen enough transactions to reproduce
 * the current state of the store.
 */
class ReplayableCommitProcess implements TransactionCommitProcess
{
    private final AtomicLong lastLocalTxId = new AtomicLong( 1 );
    private final TransactionCommitProcess localCommitProcess;
    private final TransactionCounter transactionCounter;

    ReplayableCommitProcess( TransactionCommitProcess localCommitProcess, TransactionCounter transactionCounter )
    {
        this.localCommitProcess = localCommitProcess;
        this.transactionCounter = transactionCounter;
    }

    @Override
    public long commit( TransactionToApply batch,
                        CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        long txId = lastLocalTxId.incrementAndGet();
        if ( txId > transactionCounter.lastCommittedTransactionId() )
        {
            return localCommitProcess.commit( batch, commitEvent, mode );
        }
        return txId;
    }
}
