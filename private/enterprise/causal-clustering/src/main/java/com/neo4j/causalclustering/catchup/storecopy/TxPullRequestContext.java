/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.helper.LongRange;
import com.neo4j.causalclustering.identity.StoreId;

import java.util.OptionalLong;

import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.noConstraint;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

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

    LongRange expectedFirstTxId()
    {
        return LongRange.range( startTxIdExclusive() + 1, fallbackStartId().orElse( startTxIdExclusive() ) + 1 );
    }
}
