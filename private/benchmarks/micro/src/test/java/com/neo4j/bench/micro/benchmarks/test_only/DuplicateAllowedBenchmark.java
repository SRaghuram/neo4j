/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.test_only;

import com.neo4j.bench.micro.benchmarks.core.AbstractCoreBenchmark;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Param;

@BenchmarkEnabled( true )
public class DuplicateAllowedBenchmark extends AbstractCoreBenchmark
{
    @ParamValues(
            allowed = {"standard", "standard"},
            base = {"standard"} )
    @Param( {} )
    public String DuplicateAllowedBenchmark_param;

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
