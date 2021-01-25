/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.infra.aws.AWSBatchJobLogs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AWSBatchJobLogsTest
{
    @Test
    public void formatCloudWatchURL()
    {
        String logStreamURL = AWSBatchJobLogs
                .getLogStreamURL( "eu-north-1", "macro-benchmark-job-definition-oracle8-production/default/b91bd2d1-6ede-4300-83c1-2c5c39acdb5a" );
        assertEquals( logStreamURL,
                      "https://console.aws.amazon.com/cloudwatch/home?region=eu-north-1#logEventViewer:group=/aws/batch/job;stream=macro-benchmark-job-definition-oracle8-production/default/b91bd2d1-6ede-4300-83c1-2c5c39acdb5a" );
    }
}
