/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

import org.neo4j.causalclustering.identity.StoreId;

public class GetStoreIdResponse
{
    private final StoreId storeId;

    GetStoreIdResponse( StoreId storeId )
    {
        this.storeId = storeId;
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
        GetStoreIdResponse that = (GetStoreIdResponse) o;
        return Objects.equals( storeId, that.storeId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( storeId );
    }

    @Override
    public String toString()
    {
        return "GetStoreIdResponse{" +
                "storeId=" + storeId +
                '}';
    }
}
