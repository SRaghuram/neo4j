/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import static java.lang.String.format;

public class AWSBatchJobLogs
{
    /**
     * Constructs log stream name, according to this spec, {@link https://docs.aws.amazon.com/batch/latest/userguide/job_states.html}.
     *
     * @param region AWS region name
     * @param streamName CloudWatch logs stream name
     * @return
     */
    public static String getLogStreamURL( String region, String streamName )
    {
        return format( "https://console.aws.amazon.com/cloudwatch/home?region=%s#logEventViewer:group=/aws/batch/job;stream=%s", region, streamName );
    }
}
