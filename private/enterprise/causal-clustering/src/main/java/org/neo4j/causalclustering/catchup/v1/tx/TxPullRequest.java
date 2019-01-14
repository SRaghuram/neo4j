/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.tx;

import java.util.Objects;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TxPullRequest implements DatabaseCatchupRequest
{
    private final long previousTxId;
    private final StoreId expectedStoreId;
    private final String databaseName;

    public TxPullRequest( long previousTxId, StoreId expectedStoreId, String databaseName )
    {
        if ( previousTxId < BASE_TX_ID )
        {
            throw new IllegalArgumentException( "Cannot request transaction from " + previousTxId );
        }
        this.previousTxId = previousTxId;
        this.expectedStoreId = expectedStoreId;
        this.databaseName = databaseName;
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
    public RequestMessageType messageType()
    {
        return RequestMessageType.TX_PULL_REQUEST;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
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
        TxPullRequest that = (TxPullRequest) o;
        return previousTxId == that.previousTxId && Objects.equals( expectedStoreId, that.expectedStoreId ) &&
                Objects.equals( databaseName, that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( previousTxId, expectedStoreId, databaseName );
    }

    @Override
    public String toString()
    {
        return "TxPullRequest{" + "previousTxId=" + previousTxId + ", expectedStoreId=" + expectedStoreId + ", databaseName='" + databaseName + '\'' + '}';
    }
}
