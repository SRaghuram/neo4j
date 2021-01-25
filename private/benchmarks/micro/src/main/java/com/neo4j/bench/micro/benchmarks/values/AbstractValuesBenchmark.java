/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.values;

import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;

public abstract class AbstractValuesBenchmark extends BaseRegularBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Values";
    }
}
