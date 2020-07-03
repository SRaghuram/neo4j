/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

class TxPullingContext
{
    private final TransactionCursor transactions;
    private final StoreId localStoreId;
    private final long firstTxId;
    private final long txIdPromise;
    private final TransactionIdStore transactionIdStore;

    TxPullingContext( TransactionCursor transactions, StoreId localStoreId, long firstTxId, long txIdPromise,
            TransactionIdStore transactionIdStore )
    {
        this.transactions = transactions;
        this.localStoreId = localStoreId;
        this.firstTxId = firstTxId;
        this.txIdPromise = txIdPromise;
        this.transactionIdStore = transactionIdStore;
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

    TransactionCursor transactions()
    {
        return transactions;
    }

    long lastCommittedTransactionId()
    {
        return transactionIdStore.getLastCommittedTransactionId();
    }
}
