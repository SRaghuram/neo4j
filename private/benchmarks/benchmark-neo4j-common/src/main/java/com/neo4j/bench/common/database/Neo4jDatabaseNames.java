/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import org.neo4j.configuration.GraphDatabaseSettings;

public class Neo4jDatabaseNames
{
    private static DatabaseName DEFAULT = new DatabaseName( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    private static DatabaseName SYSTEM = new DatabaseName( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

    public static DatabaseName ofNullable( String name )
    {
        if ( name == null )
        {
            return DEFAULT;
        }
        else if ( name.equals( DEFAULT.name() ) )
        {
            return DEFAULT;
        }
        else if ( name.equals( SYSTEM.name() ) )
        {
            return SYSTEM;
        }
        else
        {
            return new DatabaseName( name );
        }
    }

    public static DatabaseName defaultDatabase()
    {
        return DEFAULT;
    }

    public static DatabaseName systemDatabase()
    {
        return new DatabaseName( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
    }
}
