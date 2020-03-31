/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.tool.macro;

import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.JsonUtil;
import org.junit.Test;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfilers;
import static org.junit.Assert.assertEquals;

public class RunMacroWorkloadParamsTest
{

    @Test
    public void serializationTest()
    {
        RunMacroWorkloadParams runMacroWorkloadParams = new RunMacroWorkloadParams( "workloadName",
                                                                                    Edition.COMMUNITY,
                                                                                    Paths.get( "java" ).toAbsolutePath(),
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
                                                                                    "3.4.1",
                                                                                    "neo4jBranch",
                                                                                    "neo4jBranchOwner",
                                                                                    "toolCommit",
                                                                                    "toolOwner",
                                                                                    "toolBranch",
                                                                                    1L,
                                                                                    0L,
                                                                                    "neo4j" );
        RunMacroWorkloadParams
                actualRunMacroWorkloadParams = JsonUtil.deserializeJson( JsonUtil.serializeJson( runMacroWorkloadParams ), RunMacroWorkloadParams.class );
        assertEquals( runMacroWorkloadParams, actualRunMacroWorkloadParams );
    }
}
