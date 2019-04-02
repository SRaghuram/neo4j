/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v1.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.StoreCopyRequest;

import java.util.Objects;

import org.neo4j.storageengine.api.StoreId;

public class GetIndexFilesRequest extends StoreCopyRequest
{
    private final long indexId;

    public GetIndexFilesRequest( StoreId expectedStoreId, long indexId, long requiredTransactionId, String databaseName )
    {
        super( RequestMessageType.INDEX_SNAPSHOT, databaseName, expectedStoreId, requiredTransactionId );
        this.indexId = indexId;
    }

    public long indexId()
    {
        return indexId;
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
        GetIndexFilesRequest that = (GetIndexFilesRequest) o;
        return indexId == that.indexId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), indexId );
    }

    @Override
    public String toString()
    {
        return "GetIndexFilesRequest{" +
               "expectedStoreId=" + expectedStoreId() +
               ", indexId=" + indexId +
               ", requiredTransactionId=" + requiredTransactionId() +
               ", databaseName=" + databaseName() +
               "}";
    }
}
