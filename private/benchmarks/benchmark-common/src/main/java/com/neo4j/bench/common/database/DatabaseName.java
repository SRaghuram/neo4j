/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import java.util.Objects;

public class DatabaseName
{
    private final String databaseName;

    DatabaseName( String databaseName )
    {
        this.databaseName = Objects.requireNonNull( databaseName );
    }

    public String name()
    {
        return databaseName;
    }
}
