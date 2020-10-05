/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

public class TxPullRequest extends CatchupProtocolMessage.WithDatabaseId
{
    private final long previousTxId;
    private final StoreId expectedStoreId;

    public TxPullRequest( long previousTxId, StoreId expectedStoreId, DatabaseId databaseId )
    {
        super( RequestMessageType.TX_PULL_REQUEST, databaseId );
        if ( previousTxId < BASE_TX_ID )
        {
            throw new IllegalArgumentException( "Cannot request transaction from " + previousTxId );
        }
        this.previousTxId = previousTxId;
        this.expectedStoreId = expectedStoreId;
    }

    /**
     * Request is for transactions after this id
     */
    public long previousTxId()
    {
        return previousTxId;
    }

    public StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        TxPullRequest that = (TxPullRequest) o;
        return previousTxId == that.previousTxId &&
               Objects.equals( expectedStoreId, that.expectedStoreId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), previousTxId, expectedStoreId );
    }

    @Override
    public String toString()
    {
        return "TxPullRequest{previousTxId=" + previousTxId + ", expectedStoreId=" + expectedStoreId + ", databaseId='" + databaseId() + "'}";
    }

    @Override
    public String describe()
    {
        return getClass().getSimpleName() + " for " + databaseId() + ". Requires to pull transactions above transaction id:  " + previousTxId;
    }
}
