/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.util.Objects;

public class GetDatabaseIdRequest extends CatchupProtocolMessage
{
    private final String databaseName;

    public GetDatabaseIdRequest( String databaseName )
    {
        super( RequestMessageType.DATABASE_ID );
        this.databaseName = databaseName;
    }

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
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GetDatabaseIdRequest that = (GetDatabaseIdRequest) o;
        return type == that.type &&
               Objects.equals( databaseName, that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, databaseName );
    }

    @Override
    public String describe()
    {
        return getClass().getSimpleName() + " for " + databaseName;
    }
}
