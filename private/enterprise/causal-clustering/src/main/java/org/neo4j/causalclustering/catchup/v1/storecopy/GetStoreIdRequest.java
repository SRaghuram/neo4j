/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import java.util.Objects;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.messaging.DatabaseCatchupRequest;

public class GetStoreIdRequest implements DatabaseCatchupRequest
{
    private final String databaseName;

    public GetStoreIdRequest( String databaseName )
    {
        this.databaseName = databaseName;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.STORE_ID;
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
        if ( !(o instanceof GetStoreIdRequest) )
        {
            return false;
        }
        GetStoreIdRequest that = (GetStoreIdRequest) o;
        return Objects.equals( databaseName, that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseName );
    }
}
