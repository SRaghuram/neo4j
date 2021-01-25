/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.gbptree;

import org.neo4j.index.internal.gbptree.Layout;

abstract class TestLayout<KEY,VALUE> extends Layout.Adapter<KEY,VALUE> implements KeyValueSeeder<KEY,VALUE>
{
    protected TestLayout( boolean fixedSize, long identifier, int majorVersion, int minorVersion )
    {
        super( fixedSize, identifier, majorVersion, minorVersion );
    }
}
