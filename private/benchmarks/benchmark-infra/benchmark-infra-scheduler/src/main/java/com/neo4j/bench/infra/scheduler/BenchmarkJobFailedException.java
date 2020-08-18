/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.neo4j.bench.infra.JobStatus;

import java.util.Collection;

import static java.util.stream.Collectors.joining;

public class BenchmarkJobFailedException extends RuntimeException
{
    private final Collection<BatchBenchmarkJob> benchmarkJobs;

    public BenchmarkJobFailedException( Collection<BatchBenchmarkJob> benchmarkJobs )
    {
        super( "there are failed jobs: \n" +
               benchmarkJobs.stream()
                            .map( BatchBenchmarkJob::lastJobStatus )
                            .filter( JobStatus::isFailed )
                            .map( Object::toString )
                            .collect( joining( "\n" ) ) );
        this.benchmarkJobs = benchmarkJobs;
    }

    public Collection<BatchBenchmarkJob> benchmarkJobs()
    {
        return benchmarkJobs;
    }
}
