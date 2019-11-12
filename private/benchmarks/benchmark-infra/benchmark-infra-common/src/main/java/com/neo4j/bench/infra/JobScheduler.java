/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.infra.commands.InfraParams;

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
     * @param workerArtifactUri
     * @param baseArtifactUri
     * @param runWorkloadParams
     * @return ID of scheduled job
     */
    JobId schedule( URI workerArtifactUri, URI baseArtifactUri, RunWorkloadParams runWorkloadParams );

    /**
     * Fetches status of scheduled job
     *
     * @param jobIds
     * @return job status
     */
    List<JobStatus> jobsStatuses( List<JobId> jobIds );
}
