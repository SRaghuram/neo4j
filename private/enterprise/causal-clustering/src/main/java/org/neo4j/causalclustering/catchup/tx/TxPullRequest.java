/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;

public class TxPullRequest implements CatchUpRequest
{
    private long previousTxId;
    private final StoreId expectedStoreId;

    public TxPullRequest( long previousTxId, StoreId expectedStoreId )
    {
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
        TxPullRequest that = (TxPullRequest) o;
        return previousTxId == that.previousTxId && Objects.equals( expectedStoreId, that.expectedStoreId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( previousTxId, expectedStoreId );
    }

    @Override
    public String toString()
    {
        return String.format( "TxPullRequest{txId=%d, storeId=%s}", previousTxId, expectedStoreId );
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.TX_PULL_REQUEST;
    }
}
