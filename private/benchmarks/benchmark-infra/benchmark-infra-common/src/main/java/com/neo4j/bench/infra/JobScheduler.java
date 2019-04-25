/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.util.List;

public interface JobScheduler
{

    List<String> schedule(
            String workloads,
            String dbs,
            BenchmarkArgs args );

    List<JobStatus> jobsStatuses( List<String> jobIds );
}
