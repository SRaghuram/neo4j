/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

class TxPullingContext
{
    private final TransactionCursor transactions;
    private final StoreId localStoreId;
    private final NamedDatabaseId namedDatabaseId;
    private final long firstTxId;
    private final long txIdPromise;
    private final TransactionIdStore transactionIdStore;
    private final LogEntryWriterFactory logEntryWriterFactory;
    private final TxStreamingConstraint constraint;

    TxPullingContext( TransactionCursor transactions, StoreId localStoreId, NamedDatabaseId namedDatabaseId, long firstTxId,
            long txIdPromise, TransactionIdStore transactionIdStore, LogEntryWriterFactory logEntryWriterFactory,
            TxStreamingConstraint constraint )
    {
        this.transactions = transactions;
        this.localStoreId = localStoreId;
        this.namedDatabaseId = namedDatabaseId;
        this.firstTxId = firstTxId;
        this.txIdPromise = txIdPromise;
        this.transactionIdStore = transactionIdStore;
        this.logEntryWriterFactory = logEntryWriterFactory;
        this.constraint = constraint;
    }

    long firstTxId()
    {
        return firstTxId;
    }

    long txIdPromise()
    {
        return txIdPromise;
    }

    StoreId localStoreId()
    {
        return localStoreId;
    }

    NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
    }

    TransactionCursor transactions()
    {
        return transactions;
    }

    long lastCommittedTransactionId()
    {
        return transactionIdStore.getLastCommittedTransactionId();
    }

    LogEntryWriterFactory logEntryWriterFactory()
    {
        return logEntryWriterFactory;
    }

    public TxStreamingConstraint constraint()
    {
        return constraint;
    }
}
