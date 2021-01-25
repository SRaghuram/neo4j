/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class PrepareStoreCopyRequest extends CatchupProtocolMessage.WithDatabaseId
{
    private final StoreId storeId;

    public PrepareStoreCopyRequest( StoreId expectedStoreId, DatabaseId databaseId )
    {
        super( RequestMessageType.PREPARE_STORE_COPY, databaseId );
        this.storeId = expectedStoreId;
    }

    public StoreId storeId()
    {
        return storeId;
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
        PrepareStoreCopyRequest that = (PrepareStoreCopyRequest) o;
        return Objects.equals( storeId, that.storeId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), storeId );
    }

    @Override
    public String toString()
    {
        return "PrepareStoreCopyRequest{storeId=" + storeId + ", databaseId='" + databaseId() + "}";
    }
}
