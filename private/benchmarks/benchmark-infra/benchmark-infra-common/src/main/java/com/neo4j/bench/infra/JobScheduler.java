/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.net.URI;
import java.util.List;

/**
 * Scheduler of benchmark runs
 */
public interface JobScheduler
{

    /**
     * Schedules a benchmark run
     *
     * @param workerArtifactUri URI to worker artifact
     * @param baseArtifactUri URI to workspace base
     * @param jobName human readable job name
     * @return ID of scheduled job
     */
    JobId schedule( URI workerArtifactUri, URI baseArtifactUri, String jobName );

    /**
     * Fetches status of scheduled job
     *
     * @param jobIds
     * @return job status
     */
    List<JobStatus> jobsStatuses( List<JobId> jobIds );
}
