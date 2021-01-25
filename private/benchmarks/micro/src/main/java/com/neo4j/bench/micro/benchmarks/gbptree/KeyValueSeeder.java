/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

interface KeyValueSeeder<KEY, VALUE>
{
    KEY keyWithSeed( KEY key, long seed );

    VALUE valueWithSeed( VALUE value, long seed );

    KEY key( long seed );

    VALUE value( long seed );
}
