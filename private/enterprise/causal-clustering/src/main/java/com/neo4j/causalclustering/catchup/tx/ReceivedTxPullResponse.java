/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.storageengine.api.StoreId;

public class ReceivedTxPullResponse
{
    public static final ReceivedTxPullResponse EMPTY = new ReceivedTxPullResponse( null, null, -1 );
    private final StoreId storeId;
    private final CommittedTransactionRepresentation tx;
    private final int txSize;

    public ReceivedTxPullResponse( StoreId storeId, CommittedTransactionRepresentation tx, int txSize )
    {
        this.storeId = storeId;
        this.tx = tx;
        this.txSize = txSize;
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public CommittedTransactionRepresentation tx()
    {
        return tx;
    }

    public int txSize()
    {
        return txSize;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ReceivedTxPullResponse) )
        {
            return false;
        }
        ReceivedTxPullResponse that = (ReceivedTxPullResponse) o;
        return txSize == that.txSize &&
               Objects.equals( storeId, that.storeId ) &&
               Objects.equals( tx, that.tx );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( storeId, tx, txSize );
    }

    @Override
    public String toString()
    {
        return "ReceivedTxPullResponse{" +
               "storeId=" + storeId +
               ", txSize=" + txSize +
               '}';
    }
}
