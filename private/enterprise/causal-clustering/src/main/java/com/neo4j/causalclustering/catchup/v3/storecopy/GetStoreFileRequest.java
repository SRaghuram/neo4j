/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.StoreCopyRequest;

import java.nio.file.Path;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class GetStoreFileRequest extends StoreCopyRequest
{
    private final Path path;

    public GetStoreFileRequest( StoreId expectedStoreId, Path path, long requiredTransactionId, DatabaseId databaseId )
    {
        super( RequestMessageType.STORE_FILE, databaseId, expectedStoreId, requiredTransactionId );
        this.path = path;
    }

    public Path path()
    {
        return path;
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
        GetStoreFileRequest that = (GetStoreFileRequest) o;
        return Objects.equals( path, that.path );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), path );
    }

    @Override
    public String toString()
    {
        return "GetStoreFileRequest{" +
               "expectedStoreId=" + expectedStoreId() +
               ", file=" + path.getFileName() +
               ", requiredTransactionId=" + requiredTransactionId() +
               ", databaseId=" + databaseId() +
               "}";
    }
}
