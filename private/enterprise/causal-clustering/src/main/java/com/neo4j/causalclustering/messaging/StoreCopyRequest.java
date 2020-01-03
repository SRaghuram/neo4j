/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.catchup.RequestMessageType;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public abstract class StoreCopyRequest extends CatchupProtocolMessage.WithDatabaseId
{
    private final StoreId expectedStoreId;
    private final long requiredTransactionId;

    protected StoreCopyRequest( RequestMessageType type, DatabaseId databaseId, StoreId expectedStoreId, long requiredTransactionId )
    {
        super( type, databaseId );
        this.expectedStoreId = expectedStoreId;
        this.requiredTransactionId = requiredTransactionId;
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
        StoreCopyRequest that = (StoreCopyRequest) o;
        return requiredTransactionId == that.requiredTransactionId &&
               Objects.equals( expectedStoreId, that.expectedStoreId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), expectedStoreId, requiredTransactionId );
    }
}
