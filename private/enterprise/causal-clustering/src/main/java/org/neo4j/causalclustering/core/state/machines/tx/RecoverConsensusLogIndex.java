/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.LogProvider;

import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

/**
 * Retrieves last raft log index that was appended to the transaction log, so that raft log replay can recover while
 * preserving idempotency (avoid appending the same transaction twice).
 */
public class RecoverConsensusLogIndex
{
    private final LocalDatabase localDatabase;
    private final LogProvider logProvider;

    public RecoverConsensusLogIndex( LocalDatabase localDatabase, LogProvider logProvider )
    {
        this.localDatabase = localDatabase;
        this.logProvider = logProvider;
    }

    public long findLastAppliedIndex()
    {
        DependencyResolver dependencies = localDatabase.dataSource().getDependencyResolver();
        TransactionIdStore transactionIdStore = dependencies.resolveDependency( TransactionIdStore.class, ONLY );
        LogicalTransactionStore transactionStore = dependencies.resolveDependency( LogicalTransactionStore.class, ONLY );

        return new LastCommittedIndexFinder( transactionIdStore, transactionStore, logProvider )
                .getLastCommittedIndex();
    }
}
