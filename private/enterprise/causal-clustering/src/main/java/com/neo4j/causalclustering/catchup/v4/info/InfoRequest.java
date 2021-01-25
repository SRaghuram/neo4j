/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.util.Objects;

import org.neo4j.kernel.database.NamedDatabaseId;

public class InfoRequest extends CatchupProtocolMessage
{
    private final NamedDatabaseId namedDatabaseId;

    public InfoRequest( NamedDatabaseId namedDatabaseId )
    {
        super( RequestMessageType.INFO );
        this.namedDatabaseId = namedDatabaseId;
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
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
        InfoRequest that = (InfoRequest) o;
        return Objects.equals( namedDatabaseId, that.namedDatabaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId );
    }

    @Override
    public String toString()
    {
        return "ReconciliationInfoRequest{" +
               "databaseId=" + namedDatabaseId +
               '}';
    }

    @Override
    public String describe()
    {
        return getClass().getSimpleName();
    }
}
