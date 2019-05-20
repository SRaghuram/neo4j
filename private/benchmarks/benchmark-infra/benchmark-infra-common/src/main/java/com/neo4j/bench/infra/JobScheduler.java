/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.util.List;

/**
 * Job scheduler, which can schedule benchmarks runs.
 *
 */
public interface JobScheduler
{

    /**
     * Schedules a benchmarking runs.
     * @param workloads
     * @param dbs
     * @param args
     * @return list of scheduled job IDs.
     */
    List<String> schedule(
            String workloads,
            String dbs,
            BenchmarkArgs args );

    /**
     * Fetches statues of scheduled jobs.
     *
     * @param jobIds
     * @return job statuses
     */
    List<JobStatus> jobsStatuses( List<String> jobIds );
}
