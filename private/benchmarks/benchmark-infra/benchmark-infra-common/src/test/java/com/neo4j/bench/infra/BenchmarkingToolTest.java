/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.google.common.collect.ImmutableList;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.infra.macro.MacroToolRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BenchmarkingToolTest
{

    @Test
    public void serializationTest()
    {
        BenchmarkingTool benchmarkingTool = new BenchmarkingTool( MacroToolRunner.class,
                                                                  new RunWorkloadParams( "workloadName",
                                                                                         Edition.COMMUNITY,
                                                                                         Paths.get( "java" ).toAbsolutePath(),
                                                                                         new ArrayList<>( ImmutableList.of( ProfilerType.JFR ) ),
                                                                                         1,
                                                                                         1000,
                                                                                         Duration.ofSeconds( 1 ),
                                                                                         Duration.ofSeconds( 2 ),
                                                                                         1,
                                                                                         TimeUnit.MICROSECONDS,
                                                                                         Runtime.DEFAULT,
                                                                                         Planner.DEFAULT,
                                                                                         ExecutionMode.EXECUTE,
                                                                                         JvmArgs.empty(),
                                                                                         false,
                                                                                         false,
                                                                                         Deployment.embedded(),
                                                                                         "neo4jCommit",
                                                                                         "3.4.1",
                                                                                         "neo4jBranch",
                                                                                         "neo4jBranchOwner",
                                                                                         "toolCommit",
                                                                                         "toolOwner",
                                                                                         "toolBranch",
                                                                                         1L,
                                                                                         0L,
                                                                                         "neo4j" ) );

        String json = JsonUtil.serializeJson( benchmarkingTool );
        BenchmarkingTool actualBenchmarkingTool = JsonUtil.deserializeJson( json, BenchmarkingTool.class );
        assertEquals( benchmarkingTool, actualBenchmarkingTool );
    }
}
