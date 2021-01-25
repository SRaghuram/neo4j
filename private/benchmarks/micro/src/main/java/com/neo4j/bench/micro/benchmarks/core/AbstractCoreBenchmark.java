/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State( Scope.Benchmark )
public abstract class AbstractCoreBenchmark extends BaseDatabaseBenchmark
{
    @Override
    public String benchmarkGroup()
    {
        return "Core API";
    }
}
