/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.storageengine.api.StoreId;

public class TxPullResponse
{
    public static final TxPullResponse EMPTY = new TxPullResponse( null, null );
    private final StoreId storeId;
    private final CommittedTransactionRepresentation tx;

    public TxPullResponse( StoreId storeId, CommittedTransactionRepresentation tx )
    {
        this.storeId = storeId;
        this.tx = tx;
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public CommittedTransactionRepresentation tx()
    {
        return tx;
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

        TxPullResponse that = (TxPullResponse) o;

        return Objects.equals( storeId, that.storeId ) && Objects.equals( tx, that.tx );
    }

    @Override
    public int hashCode()
    {
        int result = storeId != null ? storeId.hashCode() : 0;
        result = 31 * result + (tx != null ? tx.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "TxPullResponse{storeId=%s, tx=%s}", storeId, tx );
    }
}
