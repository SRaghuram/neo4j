/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.common;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Objects.requireNonNull;

public class DatabaseDroppedMessage
{
    private final DatabaseId databaseId;

    public DatabaseDroppedMessage( DatabaseId databaseId )
    {
        this.databaseId = requireNonNull( databaseId );
    }

    public DatabaseId databaseId()
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
        DatabaseDroppedMessage that = (DatabaseDroppedMessage) o;
        return Objects.equals( databaseId, that.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId );
    }

    @Override
    public String toString()
    {
        return "DatabaseDroppedMessage{" + "databaseId=" + databaseId + '}';
    }
}
