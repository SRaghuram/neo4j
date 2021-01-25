/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.OptionalLong;

import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.noConstraint;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class TxPullRequestContext
{
    private final RequiredTransactions requiredTransactions;
    private final StoreId expectedStoreId;
    private final boolean hasFallbackStartId;

    static TxPullRequestContext createContextFromStoreCopy( RequiredTransactions requiredTransactions, StoreId expectedStoreId )
    {
        return new TxPullRequestContext( requiredTransactions, expectedStoreId );
    }

    /**
     * <p>
     * If there are no transaction logs then we look at where the metadata store points to.
     * This would be the case for example with a backup taken during an idle period of the system.
     *
     * If our store is not empty we must pull from the metaDataStoreIndex either exclusively or
     * inclusively, depending on the possible states of the remote store's transaction log. These
     * possible states are:
     *
     * Assume MetaDataStore have index=N then if:
     *
     * 1. Remote store has Tx log containing ids up to N. In this case we must pull from N (inclusively) in order to get the latest remote transaction.
     * 2. Remote store has Tx log containing ids from N+1 (if the remote store was seeded from the same store as ours then tx log will star at N+1).
     *  In this case we must pull from N (exclusively) in order to get the latest remote transaction.
     * 3. Remote store has Tx log containing both N and N+1. In this case any solution may work.
     *
     * <p>
     * However, if there are transaction logs then we want to find out where
     * they end and pull from there, excluding the last one so that we do not
     * get duplicate entries.
     */
    static TxPullRequestContext createContextFromCatchingUp( StoreId expectedStoreId, CommitState commitState )
    {
        if ( commitState.transactionLogIndex().isPresent() )
        {
            return new TxPullRequestContext( noConstraint( commitState.transactionLogIndex().get() + 1 ), expectedStoreId );
        }
        else
        {
            long metaDataStoreIndex = commitState.metaDataStoreIndex();
            if ( metaDataStoreIndex == BASE_TX_ID )
            {
                return new TxPullRequestContext( noConstraint( metaDataStoreIndex + 1 ), expectedStoreId );
            }
            else
            {
                return new TxPullRequestContext( noConstraint( metaDataStoreIndex ), expectedStoreId, true );
            }
        }
    }

    private TxPullRequestContext( RequiredTransactions requiredTransactions, StoreId expectedStoreId, boolean hasFallbackStartId )
    {
        this.requiredTransactions = requiredTransactions;
        this.expectedStoreId = expectedStoreId;
        this.hasFallbackStartId = hasFallbackStartId;
    }

    private TxPullRequestContext( RequiredTransactions requiredTransactions, StoreId expectedStoreId )
    {
        this( requiredTransactions, expectedStoreId, false );
    }

    OptionalLong fallbackStartId()
    {
        return hasFallbackStartId ? OptionalLong.of( startTxIdExclusive() + 1 ) : OptionalLong.empty();
    }

    long startTxIdExclusive()
    {
        return requiredTransactions.startTxId() - 1;
    }

    StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    boolean constraintReached( long lastWrittenTx )
    {
        return requiredTransactions.noRequiredTxId() || (requiredTransactions.requiredTxId() <= lastWrittenTx);
    }
}
