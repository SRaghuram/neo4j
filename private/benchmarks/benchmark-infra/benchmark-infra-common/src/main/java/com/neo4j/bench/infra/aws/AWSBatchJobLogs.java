/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.neo4j.bench.infra.JobId;

import static java.lang.String.format;

public class AWSBatchJobLogs
{
    /**
     * Constructs log stream name, according to this spec, {@link https://docs.aws.amazon.com/batch/latest/userguide/job_states.html}.
     * @param jobDefinition
     * @param jobId
     * @return
     */
    public static String getLogStreamName( String jobDefinition, JobId jobId )
    {
        return format( "%s/default/%s", jobDefinition, jobId.id() );
    }
}
