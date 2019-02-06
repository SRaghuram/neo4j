/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

public enum Neo4jImporter
{
    BATCH,
    PARALLEL;

    public static Neo4jImporter defaultImporter()

    {
        return BATCH;
    }
}
