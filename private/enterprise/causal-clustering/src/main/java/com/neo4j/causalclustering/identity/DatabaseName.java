/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

/**
 * TODO Is this needed given {@link DatabaseId}?
 * Simple wrapper class for database name strings. These values are provided using the
 * {@link CausalClusteringSettings#database } setting.
 */
public class DatabaseName
{
    private final String name;

    public DatabaseName( DatabaseId databaseId )
    {
        this.name = databaseId.name();
    }

    private DatabaseName( String name )
    {
        this.name = name;
    }

    public String name()
    {
        return name;
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
        DatabaseName that = (DatabaseName) o;
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    public static class Marshal extends SafeStateMarshal<DatabaseName>
    {
        @Override
        protected DatabaseName unmarshal0( ReadableChannel channel ) throws IOException
        {
            return new DatabaseName( StringMarshal.unmarshal( channel ) );
        }

        @Override
        public void marshal( DatabaseName databaseName, WritableChannel channel ) throws IOException
        {
            StringMarshal.marshal( channel, databaseName.name() );
        }

        @Override
        public DatabaseName startState()
        {
            return null;
        }

        @Override
        public long ordinal( DatabaseName databaseName )
        {
            return databaseName == null ? 0 : 1;
        }
    }
}
