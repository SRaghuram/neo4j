/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.identity.StoreId;

import java.util.OptionalLong;

import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactionRange.single;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TxPullRequestContext
{
    private final RequiredTransactionRange requiredTransactionRange;
    private final StoreId expectedStoreId;
    private final boolean hasFallbackStartId;

    static TxPullRequestContext createContextFromStoreCopy( RequiredTransactionRange requiredTransactionRange, StoreId expectedStoreId )
    {
        return new TxPullRequestContext( requiredTransactionRange, expectedStoreId );
    }

    /**
     * Later stages of the startup process require at least one transaction to
     * figure out the mapping between the transaction log and the consensus log.
     * <p>
     * If there are no transaction logs then we can pull from and including
     * the index which the metadata store points to. This would be the case
     * for example with a backup taken during an idle period of the system.
     * <p>
     * However, if there are transaction logs then we want to find out where
     * they end and pull from there, excluding the last one so that we do not
     * get duplicate entries.
     */
    static TxPullRequestContext createContextFromCatchingUp( StoreId expectedStoreId, CommitState commitState )
    {
        if ( commitState.transactionLogIndex().isPresent() )
        {
            return new TxPullRequestContext( single( commitState.transactionLogIndex().get() + 1 ), expectedStoreId );
        }
        else
        {
            long metaDatStoreIndex = commitState.metaDataStoreIndex();
            if ( metaDatStoreIndex == BASE_TX_ID )
            {
                return new TxPullRequestContext( single( metaDatStoreIndex + 1 ), expectedStoreId );
            }
            else
            {
                return new TxPullRequestContext( single( metaDatStoreIndex ), expectedStoreId, true );
            }
        }
    }

    private TxPullRequestContext( RequiredTransactionRange requiredTransactionRange, StoreId expectedStoreId, boolean hasFallbackStartId )
    {
        this.requiredTransactionRange = requiredTransactionRange;
        this.expectedStoreId = expectedStoreId;
        this.hasFallbackStartId = hasFallbackStartId;
    }

    private TxPullRequestContext( RequiredTransactionRange requiredTransactionRange, StoreId expectedStoreId )
    {
        this( requiredTransactionRange, expectedStoreId, false );
    }

    OptionalLong fallbackStartId()
    {
        return hasFallbackStartId ? OptionalLong.of( startTxId() + 1 ) : OptionalLong.empty();
    }

    public long startTxId()
    {
        return requiredTransactionRange.startTxId() - 1;
    }

    StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    boolean constraintReached( long lastWrittenTx )
    {
        return (requiredTransactionRange.requiredTxId() == -1) || (requiredTransactionRange.requiredTxId() <= lastWrittenTx);
    }

    RequiredTransactionRange expectedFirstTxId()
    {
        return RequiredTransactionRange.range( startTxId() + 1, fallbackStartId().orElse( startTxId() ) + 1 );
    }
}
