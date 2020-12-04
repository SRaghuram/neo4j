/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.common.tool.micro.RunMicroWorkloadParams;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.infra.macro.MacroToolRunner;
import com.neo4j.bench.infra.micro.MicroToolRunner;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfilers;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BenchmarkingToolTest
{

    @Test
    public void macroParamsSerializationTest()
    {
        BenchmarkingTool benchmarkingTool = new BenchmarkingTool( MacroToolRunner.class,
                                                                  new RunToolMacroWorkloadParams(
                                                                          new RunMacroWorkloadParams( "workloadName",
                                                                                                      emptyList(),
                                                                                                      Edition.COMMUNITY,
                                                                                                      Paths.get( "java" )
                                                                                                           .toAbsolutePath(),
                                                                                                      defaultProfilers( ProfilerType.JFR ),
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
                                                                                                      new Version( "3.4.1" ),
                                                                                                      "neo4jBranch",
                                                                                                      "neo4jBranchOwner",
                                                                                                      1L,
                                                                                                      0L,
                                                                                                      "neo4j" ),
                                                                          "storeName" ) );

        String json = JsonUtil.serializeJson( benchmarkingTool );
        BenchmarkingTool actualBenchmarkingTool = JsonUtil.deserializeJson( json, BenchmarkingTool.class );
        assertEquals( benchmarkingTool, actualBenchmarkingTool );
    }

    @Test
    public void microParamsSerializationTest()
    {
        BenchmarkingTool benchmarkingTool = new BenchmarkingTool( MicroToolRunner.class,
                                                                  RunMicroWorkloadParams.create( "neo4jCommit",
                                                                                                 "3.4.4", // neo4jVersion,
                                                                                                 Edition.COMMUNITY,
                                                                                                 "3.4.4", // neo4jBranch
                                                                                                 "branchOwner",
                                                                                                 new File( "neo4j.conf" ).getAbsoluteFile(),
                                                                                                 "toolCommit",
                                                                                                 "toolBranchOwner",
                                                                                                 "3.4", // tool branch
                                                                                                 1L, // teamcity build
                                                                                                 2L, // parent teamcity build
                                                                                                 JvmArgs.from( "-Xmx4g" ).toArgsString(),
                                                                                                 new File( "config" ).getAbsoluteFile(),
                                                                                                 "", // jmh args,
                                                                                                 "GC",
                                                                                                 ErrorReporter.ErrorPolicy.FAIL,
                                                                                                 Paths.get( "java" ).toAbsolutePath().toFile(),
                                                                                                 "triggeredBy"
                                                                  ) );

        String json = JsonUtil.serializeJson( benchmarkingTool );
        BenchmarkingTool actualBenchmarkingTool = JsonUtil.deserializeJson( json, BenchmarkingTool.class );
        assertEquals( benchmarkingTool, actualBenchmarkingTool );
    }
}
