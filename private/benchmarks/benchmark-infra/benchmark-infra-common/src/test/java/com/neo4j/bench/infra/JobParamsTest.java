/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.infra.macro.MacroToolRunner;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfilers;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobParamsTest
{

    @Test
    public void serializeAndDeserialize( @TempDir Path tempDir ) throws IOException
    {
        // given
        JobParams<RunToolMacroWorkloadParams> jobParams = new JobParams<>(
                new InfraParams(
                        new AWSCredentials( "awsKey",
                                            "awsSecret",
                                            "awsRegion" ),
                        "resultsStoreUsername",
                        "resultsStorePassword",
                        URI.create( "bolt://localhost/" ),
                        URI.create( "s3://benchmarking.com/123456" ),
                        ErrorReportingPolicy.REPORT_THEN_FAIL,
                        Workspace.create( tempDir ).build() ),
                new BenchmarkingRun<>(
                        new BenchmarkingTool<>(
                                MacroToolRunner.class,
                                new RunToolMacroWorkloadParams(
                                        new RunMacroWorkloadParams( "workloadName",
                                                                    emptyList(),
                                                                    Edition.ENTERPRISE,
                                                                    Paths.get( "jvm" ).toAbsolutePath(),
                                                                    defaultProfilers( ProfilerType.GC, ProfilerType.JFR ),
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
                                                                    new Version( "3.4.12" ),
                                                                    "neo4jBranch",
                                                                    "neo4jBranchOwner",
                                                                    123456L,
                                                                    123455L,
                                                                    "triggeredBy" ),
                                        "storeName",
                                        URI.create( "s3://dataSetBaseUri/" ) ) ),
                        UUID.randomUUID().toString() ) );
        // when
        JobParams<?> actual = JsonUtil.deserializeJson( JsonUtil.serializeJson( jobParams ), JobParams.class );
        // then
        assertEquals( jobParams, actual );
    }

    @Test
    public void serializeAndDeserializeServer( @TempDir Path tempDir ) throws IOException
    {
        // given
        JobParams<RunToolMacroWorkloadParams> jobParams = new JobParams<>(
                new InfraParams(
                        new AWSCredentials( "awsKey",
                                            "awsSecret",
                                            "awsRegion" ),
                        "resultsStoreUsername",
                        "resultsStorePassword",
                        URI.create( "bolt://localhost/" ),
                        URI.create( "s3://benchmarking.com/123456" ),
                        ErrorReportingPolicy.REPORT_THEN_FAIL,
                        Workspace.create( Files.createTempDirectory( tempDir, "workspace" ) ).build() ),
                new BenchmarkingRun<>(
                        new BenchmarkingTool<>(
                                MacroToolRunner.class,
                                new RunToolMacroWorkloadParams(
                                        new RunMacroWorkloadParams( "workloadName",
                                                                    emptyList(),
                                                                    Edition.ENTERPRISE,
                                                                    Paths.get( "jvm" ).toAbsolutePath(),
                                                                    defaultProfilers( ProfilerType.GC, ProfilerType.JFR ),
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
                                                                    Deployment.server( Files.createTempDirectory( tempDir, "server" ).toString() ),
                                                                    "neo4jCommit",
                                                                    new Version( "3.4.12" ),
                                                                    "neo4jBranch",
                                                                    "neo4jBranchOwner",
                                                                    123456L,
                                                                    123455L,
                                                                    "triggeredBy" ),
                                        "storeName",
                                        URI.create( "s3://dataSetBaseUri/" ) ) ),
                        UUID.randomUUID().toString() ) );
        // when

        JobParams<?> actual = JsonUtil.deserializeJson( JsonUtil.serializeJson( jobParams ), JobParams.class );
        // then
        assertEquals( jobParams, actual );
    }
}
