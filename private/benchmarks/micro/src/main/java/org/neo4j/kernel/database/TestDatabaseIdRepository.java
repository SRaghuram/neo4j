/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package org.neo4j.kernel.database;

import org.neo4j.configuration.GraphDatabaseSettings;

public class TestDatabaseIdRepository implements DatabaseIdRepository
{
    private static DatabaseId DEFAULT_DATABASE_ID = new DatabaseId( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    private static DatabaseId SYSTEM_DATABASE_ID = new DatabaseId( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

    @Override
    public DatabaseId get( String databaseName )
    {
        return new DatabaseId( databaseName );
    }

    @Override
    public DatabaseId defaultDatabase()
    {
        return DEFAULT_DATABASE_ID;
    }

    @Override
    public DatabaseId systemDatabase()
    {
        return SYSTEM_DATABASE_ID;
    }
}
