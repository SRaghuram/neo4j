/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import java.util.Objects;

public class DatabaseName
{
    private static final String SYSTEM_DATABASE_NAME = "system";
    private static final String DEFAULT_DATABASE_NAME = "neo4j";

    private final String databaseName;

    public static DatabaseName ofNullable( String name )
    {
        return new DatabaseName( name == null ? DEFAULT_DATABASE_NAME : name );
    }

    public static DatabaseName defaultDatabase()
    {
        return new DatabaseName( DEFAULT_DATABASE_NAME );
    }

    public static DatabaseName systemDatabase()
    {
        return new DatabaseName( SYSTEM_DATABASE_NAME );
    }

    private DatabaseName( String databaseName )
    {
        this.databaseName = Objects.requireNonNull( databaseName );
    }

    public String name()
    {
        return databaseName;
    }
}
