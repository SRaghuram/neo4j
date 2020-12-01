/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.Objects;

import org.neo4j.kernel.database.NamedDatabaseId;

public final class DatabasePanicEvent extends AbstractPanicEvent
{

    private final NamedDatabaseId databaseId;

    public DatabasePanicEvent( NamedDatabaseId databaseId, DatabasePanicReason reason, Throwable cause )
    {
        super( cause, reason );
        this.databaseId = databaseId;
    }

    public NamedDatabaseId databaseId()
    {
        return databaseId;
    }

    @Override
    public String toString()
    {
        return "DatabasePanicInfo{" +
               "cause=" + cause +
               ", reason=" + reason +
               ", databaseId=" + databaseId +
               '}';
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
        DatabasePanicEvent event = (DatabasePanicEvent) o;
        return super.equals( event ) && databaseId.equals( event.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), databaseId );
    }
}
