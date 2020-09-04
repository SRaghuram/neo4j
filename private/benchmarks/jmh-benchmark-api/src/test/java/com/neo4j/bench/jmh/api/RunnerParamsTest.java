/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunnerParamsTest
{

    @Test
    public void shouldCreateBaseRunnerParams( @TempDir Path tempDir ) throws IOException
    {
        // when
        RunnerParams runnerParams = RunnerParams.create( tempDir );

        Set<String> paramKeys = runnerParams.asList()
                                            .stream()
                                            .map( RunnerParams.RunnerParam::name )
                                            .collect( toSet() );

        // then
        HashSet<String> expectedParamKeys = newHashSet( RunnerParams.PARAM_RUNNER_PARAMS,
                                                        RunnerParams.PARAM_WORK_DIR,
                                                        RunnerParams.PARAM_RUN_ID,
                                                        RunnerParams.PARAM_PROFILERS );

        assertThat( paramKeys, equalTo( expectedParamKeys ) );
        assertThat( runnerParams.workDir(), equalTo( tempDir ) );
        assertThat( runnerParams.profilerTypes(), equalTo( Collections.emptyList() ) );
    }

    @Test
    public void shouldBeImmutableWhenUpdating( @TempDir Path tempDir ) throws IOException
    {
        // given
        HashSet<String> baseParamKeys = newHashSet( RunnerParams.PARAM_RUNNER_PARAMS,
                                                    RunnerParams.PARAM_WORK_DIR,
                                                    RunnerParams.PARAM_RUN_ID,
                                                    RunnerParams.PARAM_PROFILERS );

        // when
        RunnerParams runnerParams0 = RunnerParams.create( tempDir );

        // update profiler types
        List<ProfilerType> expectedProfilerTypes1 = Lists.newArrayList( ProfilerType.GC, ProfilerType.ASYNC );
        RunnerParams runnerParams1 =
                runnerParams0.copyWithProfilers( ParameterizedProfiler.defaultProfilers( expectedProfilerTypes1 ) );
        // update run ID
        RunnerParams runnerParams2 = runnerParams1.copyWithNewRunId();
        // add parameter
        RunnerParams runnerParams3 = runnerParams2.copyWithParam( "foo", "bar" );
        Set<String> expectedParamKeys3 = Sets.newHashSet( baseParamKeys );
        expectedParamKeys3.add( "foo" );

        // then

        // profilers should have been updated
        Set<String> paramKeys1 = runnerParams1.asList()
                                              .stream()
                                              .map( RunnerParams.RunnerParam::name )
                                              .collect( toSet() );
        assertThat( paramKeys1, equalTo( baseParamKeys ) );
        assertThat( runnerParams1.workDir(), equalTo( runnerParams0.workDir() ) );
        assertThat( runnerParams1.runId(), equalTo( runnerParams0.runId() ) );
        assertThat( runnerParams1.profilerTypes(), equalTo( expectedProfilerTypes1 ) );
        paramKeys1.forEach( name -> assertTrue( runnerParams1.containsParam( name ) ) );

        // run ID of new runner parameters should NOT equal run ID of previous runner parameters
        Set<String> paramKeys2 = runnerParams2.asList()
                                              .stream()
                                              .map( RunnerParams.RunnerParam::name )
                                              .collect( toSet() );
        assertThat( paramKeys2, equalTo( baseParamKeys ) );
        assertThat( runnerParams2.workDir(), equalTo( runnerParams1.workDir() ) );
        assertThat( runnerParams2.runId(), not( equalTo( runnerParams1.runId() ) ) );
        assertThat( runnerParams2.profilerTypes(), equalTo( runnerParams1.profilerTypes() ) );
        paramKeys2.forEach( name -> assertTrue( runnerParams2.containsParam( name ) ) );

        // new runner parameters should contain an additional parameter, called 'foo'
        Set<String> paramKeys3 = runnerParams3.asList()
                                              .stream()
                                              .map( RunnerParams.RunnerParam::name )
                                              .collect( toSet() );
        assertThat( paramKeys3, equalTo( expectedParamKeys3 ) );
        assertThat( runnerParams3.workDir(), equalTo( runnerParams2.workDir() ) );
        assertThat( runnerParams3.runId(), equalTo( runnerParams2.runId() ) );
        assertThat( runnerParams3.profilerTypes(), equalTo( runnerParams2.profilerTypes() ) );
        paramKeys3.forEach( name -> assertTrue( runnerParams3.containsParam( name ) ) );
        RunnerParams.RunnerParam param3 = runnerParams3.asList()
                                                       .stream()
                                                       .filter( param -> param.name().equals( "foo" ) )
                                                       .findFirst()
                                                       .orElseThrow( () -> new RuntimeException( "Expected to find parameter with key 'name'" ) );
        assertThat( param3.value(), equalTo( "bar" ) );
    }
}
