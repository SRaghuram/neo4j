/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.benchmarks.invalid;

import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import org.openjdk.jmh.annotations.Param;

@BenchmarkEnabled( true )
public class DuplicateBaseBenchmark extends BaseBenchmark
{
    @ParamValues(
            allowed = {"standard"},
            base = {"standard", "standard"} )
    @Param( {} )
    public String DuplicateBaseBenchmark_param;

    @Override
    public String benchmarkGroup()
    {
        return "Example";
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }
}
