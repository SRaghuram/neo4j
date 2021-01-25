/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.nio.file.Path;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class GetStoreFileRequest extends CatchupProtocolMessage.WithDatabaseId
{
    private final Path path;
    private final StoreId expectedStoreId;
    private final long requiredTransactionId;

    public GetStoreFileRequest( StoreId expectedStoreId, Path path, long requiredTransactionId, DatabaseId databaseId )
    {
        super( RequestMessageType.STORE_FILE, databaseId );
        this.expectedStoreId = expectedStoreId;
        this.requiredTransactionId = requiredTransactionId;
        this.path = path;
    }

    public Path path()
    {
        return path;
    }

    public final StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    public final long requiredTransactionId()
    {
        return requiredTransactionId;
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
        return requiredTransactionId == that.requiredTransactionId &&
               Objects.equals( path, that.path ) &&
               Objects.equals( expectedStoreId, that.expectedStoreId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), path, expectedStoreId, requiredTransactionId );
    }

    @Override
    public String describe()
    {
        return getClass().getSimpleName() + " for " + databaseId() + ". Requesting file: " + path.getFileName() + " with required minimum transaction id: " +
               requiredTransactionId();
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
