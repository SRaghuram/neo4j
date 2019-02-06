/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.tempindex;

public interface TempIndex
{
    void put( long k, long v );

    long get( long k );

    void shutdown();
}
