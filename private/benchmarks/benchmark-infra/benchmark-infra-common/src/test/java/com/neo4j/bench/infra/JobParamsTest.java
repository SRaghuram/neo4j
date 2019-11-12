/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.infra.commands.InfraParams;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class JobParamsTest
{

    @Test
    public void serializeAndDeserialize() throws IOException
    {
        // given
        JobParams jobParams = new JobParams(
                new InfraParams(
                        "awsSecret",
                        "awsKey",
                        "awsRegion",
                        "storeName",
                        "resultsStoreUsername",
                        "resultsStorePassword",
                        URI.create( "bolt://localhost/" ),
                        URI.create( "s3://benchmarking.com/123456" ),
                        ErrorReportingPolicy.REPORT_THEN_FAIL ),
                new RunWorkloadParams( "workloadName",
                                       Edition.ENTERPRISE,
                                       Paths.get( "jvm" ).toAbsolutePath(),
                                       Arrays.asList( ProfilerType.GC, ProfilerType.JFR ),
                                       1,
                                       1,
                                       Duration.ofMillis( 1 ),
                                       Duration.ofMillis( 2 ),
                                       1,
                                       TimeUnit.MILLISECONDS,
                                       Runtime.DEFAULT,
                                       Planner.DEFAULT,
                                       ExecutionMode.EXECUTE,
                                       JvmArgs.from( "-Xmx4g", "-Xms4g" ),
                                       false,
                                       false,
                                       Deployment.embedded(),
                                       "neo4jCommit",
                                       "3.4.12",
                                       "neo4jBranch",
                                       "neo4jBranchOwner",
                                       "toolCommit",
                                       "toolOwner",
                                       "toolBranch",
                                       123456L,
                                       123455L,
                                       "triggeredBy" ) );
        // when

        JobParams actual = JobParams.fromJson( JsonUtil.serializeJson( jobParams ) );
        // then
        assertEquals( jobParams, actual );
    }
}
