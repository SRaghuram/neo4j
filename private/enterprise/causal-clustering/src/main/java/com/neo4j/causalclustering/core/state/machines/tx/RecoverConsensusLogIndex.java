/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Retrieves last raft log index that was appended to the transaction log, so that raft log replay can recover while
 * preserving idempotency (avoid appending the same transaction twice).
 */
public class RecoverConsensusLogIndex
{
    private final LogProvider logProvider;
    private final Supplier<TransactionIdStore> txIdStore;
    private final Supplier<LogicalTransactionStore> txStore;

    public RecoverConsensusLogIndex( Supplier<TransactionIdStore> txIdStore, Supplier<LogicalTransactionStore> txStore, LogProvider logProvider )
    {
        this.txIdStore = txIdStore;
        this.txStore = txStore;
        this.logProvider = logProvider;
    }

    public long findLastAppliedIndex()
    {
        return new LastCommittedIndexFinder( txIdStore.get(), txStore.get(), logProvider ).getLastCommittedIndex();
    }
}
