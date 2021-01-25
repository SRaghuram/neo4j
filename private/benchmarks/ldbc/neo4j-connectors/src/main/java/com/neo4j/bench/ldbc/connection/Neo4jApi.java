/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

public enum Neo4jApi
{
    EMBEDDED_CORE,
    EMBEDDED_CYPHER,
    REMOTE_CYPHER;

    public boolean isRemote()
    {
        return equals( REMOTE_CYPHER );
    }
}
