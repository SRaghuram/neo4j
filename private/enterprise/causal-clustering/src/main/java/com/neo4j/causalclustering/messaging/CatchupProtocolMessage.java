/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.catchup.RequestMessageType;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public abstract class CatchupProtocolMessage implements Message
{
    private final RequestMessageType type;
    private final DatabaseId databaseId;

    protected CatchupProtocolMessage( RequestMessageType type, DatabaseId databaseId )
    {
        this.type = type;
        this.databaseId = databaseId;
    }

    public final RequestMessageType messageType()
    {
        return type;
    }

    public final DatabaseId databaseId()
    {
        return databaseId;
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
        CatchupProtocolMessage that = (CatchupProtocolMessage) o;
        return type == that.type &&
               Objects.equals( databaseId, that.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, databaseId );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{type=" + type + ", databaseId='" + databaseId + "'}";
    }
}
