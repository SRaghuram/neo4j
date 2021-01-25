/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.macro.workload.Query;

public class ForkFailureException extends Exception
{
    private final Query query;
    private final Throwable cause;
    private final BenchmarkDirectory benchmarkDir;

    ForkFailureException( Query query, BenchmarkDirectory benchmarkDir, Throwable cause )
    {
        super( cause );
        this.query = query;
        this.benchmarkDir = benchmarkDir;
        this.cause = cause;
    }

    public Query query()
    {
        return query;
    }

    public BenchmarkDirectory benchmarkDir()
    {
        return benchmarkDir;
    }

    @Override
    public Throwable getCause()
    {
        return cause;
    }
}
