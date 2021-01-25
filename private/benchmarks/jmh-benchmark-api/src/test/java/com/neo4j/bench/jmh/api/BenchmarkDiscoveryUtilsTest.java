/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.jmh.api.benchmarks.valid.ValidDisabledBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark2;
import com.neo4j.bench.jmh.api.config.ParameterValue;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.THREADS_PARAM;
import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.extractParameterValues;
import static com.neo4j.bench.jmh.api.BenchmarkDiscoveryUtils.parametersAsMap;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkDiscoveryUtilsTest
{
    @Test
    void shouldReadDescription()
    {
        String description = BenchmarkDiscoveryUtils.descriptionFor( ValidEnabledBenchmark1.class );

        assertThat( description, equalTo( ValidEnabledBenchmark1.class.getSimpleName() ) );
    }

    @Test
    void shouldReadIsEnabled()
    {
        boolean benchmark1IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidEnabledBenchmark1.class );
        boolean benchmark2IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidEnabledBenchmark2.class );
        boolean benchmark3IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidDisabledBenchmark.class );

        assertTrue( benchmark1IsEnabled );
        assertTrue( benchmark2IsEnabled );
        assertFalse( benchmark3IsEnabled );
    }

    @Test
    void shouldReadBenchmarkGroup()
    {
        String group = BenchmarkDiscoveryUtils.benchmarkGroupFor( ValidEnabledBenchmark1.class );

        assertThat( group, equalTo( "Example" ) );
    }

    @Test
    void shouldReadBenchmarkIsThreadSafe()
    {
        boolean benchmark1IsThreadSafe = BenchmarkDiscoveryUtils.isThreadSafe( ValidEnabledBenchmark1.class );
        boolean benchmark2IsThreadSafe = BenchmarkDiscoveryUtils.isThreadSafe( ValidEnabledBenchmark2.class );

        assertFalse( benchmark1IsThreadSafe );
        assertTrue( benchmark2IsThreadSafe );
    }

    @Test
    void shouldThrowExceptionWhenDuplicatesInParameterValues()
    {
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k1", "v1" ) ) ) );
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k1", "v2" ) ) ) );
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v2" ),
                new ParameterValue( "k2", "v2" ) ) ) );

        assertFalse( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v2" ) ) ) );
        assertFalse( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v1" ) ) ) );
    }

    private boolean throwsExceptionWhenParameterValuesContainsDuplicates( List<ParameterValue> parameterValues )
    {
        try
        {
            parametersAsMap( parameterValues );
            return false;
        }
        catch ( Throwable e )
        {
            return true;
        }
    }

    @Test
    void shouldExtractJmhParamsAndThreadsCorrectly()
    {
        int threads = 42;
        ParameterValue param1 = new ParameterValue( "param1", "value" );
        ParameterValue param2 = new ParameterValue( "param2", "value" );
        ParameterValue param3 = new ParameterValue( "param3", "value" );
        ParameterValue paramThreads = new ParameterValue( THREADS_PARAM, Integer.toString( threads ) );

        RunnerParams runnerParams = RunnerParams.create( Paths.get( "work_dir" ) )
                                                .copyWithParam( "system_param_1", "value_1" );

        WorkloadParams workloadParams = new WorkloadParams();
        workloadParams.put( param1.param(), param1.value(), 1 );
        workloadParams.put( param2.param(), param2.value(), 1 );
        workloadParams.put( param3.param(), param3.value(), 1 );
        runnerParams.asList()
                    .forEach( runnerParam -> workloadParams.put( runnerParam.name(), runnerParam.value(), 1 ) );

        BenchmarkParams benchmarkParams = createBenchmarkParams( paramThreads, workloadParams,
                                                                 "com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1.methodOne",
                                                                 Collections.emptyList() );

        List<ParameterValue> parameterValues = extractParameterValues( benchmarkParams, runnerParams );

        assertTrue( parameterValues.contains( param1 ) );
        assertTrue( parameterValues.contains( param2 ) );
        assertTrue( parameterValues.contains( param3 ) );
        assertTrue( parameterValues.contains( paramThreads ) );
        assertThat(
                parameterValues.stream().map( ParameterValue::toString ).collect( joining( "\n" ) ),
                parameterValues.size(),
                equalTo( 4 ) );
    }

    @Test
    void shouldCreateBenchmarks()
    {
        ParameterValue paramThreads = new ParameterValue( THREADS_PARAM, "1" );
        RunnerParams runnerParams = RunnerParams.create( Paths.get( "." ) );
        WorkloadParams workloadParams = new WorkloadParams();
        List<String> threadGroupLabels = Collections.emptyList();
        BenchmarkParams benchmarkParams = createBenchmarkParams( paramThreads, workloadParams,
                                                                 "com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1.methodOne",
                                                                 threadGroupLabels );

        Benchmarks benchmarks = BenchmarkDiscoveryUtils.toBenchmarks( benchmarkParams, runnerParams );

        assertThat( benchmarks.parentBenchmark().simpleName(), equalTo( "ValidEnabledBenchmark1.methodOne" ) );
        assertFalse( benchmarks.isGroup() );
    }

    @Test
    void shouldCreateChildBenchmarksForGroup()
    {
        ParameterValue paramThreads = new ParameterValue( THREADS_PARAM, "1" );
        RunnerParams runnerParams = RunnerParams.create( Paths.get( "." ) );
        WorkloadParams workloadParams = new WorkloadParams();
        List<String> threadGroupLabels = Arrays.asList( "methodOne", "methodTwo" );
        BenchmarkParams benchmarkParams = createBenchmarkParams( paramThreads, workloadParams,
                                                                 "com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledGroupBenchmark.group",
                                                                 threadGroupLabels );

        Benchmarks benchmarks = BenchmarkDiscoveryUtils.toBenchmarks( benchmarkParams, runnerParams );

        assertThat( benchmarks.parentBenchmark().simpleName(), equalTo( "ValidEnabledGroupBenchmark.group" ) );
        assertTrue( benchmarks.isGroup() );
        assertTrue( benchmarks.hasChildBenchmarkWith( "methodOne" ), "contains benchmark with label 'methodOne'" );
        assertTrue( benchmarks.hasChildBenchmarkWith( "methodTwo" ), "contains benchmark with label 'methodTwo'" );
        assertFalse( benchmarks.hasChildBenchmarkWith( "methodThree" ), "not contain benchmark with label 'methodThree'" );
    }

    private BenchmarkParams createBenchmarkParams( ParameterValue paramThreads, WorkloadParams workloadParams, String benchmark,
                                                   List<String> threadGroupLabels )
    {
        return new BenchmarkParams(
                benchmark, /* benchmark */
                "generatedDoThings", /* generatedTarget */
                false, /* syncIterations */
                Integer.parseInt( paramThreads.value() ),
                new int[]{},/* threadGroups */
                threadGroupLabels, /* threadGroupLabels */
                1, /* forks */
                1, /* warmupForks */
                new IterationParams(
                        IterationType.WARMUP,
                        1, /* count */
                        TimeValue.seconds( 5 ), /* time */
                        1 /* batchSize */ ),
                new IterationParams(
                        IterationType.MEASUREMENT,
                        1, /* count */
                        TimeValue.seconds( 5 ), /* time */
                        1 /* batchSize */ ),
                org.openjdk.jmh.annotations.Mode.AverageTime, /* mode */
                workloadParams, /* workload params */
                TimeUnit.DAYS, /* time unit */
                1, /* opsPerInvocation */
                "jvm", /* jvm */
                Collections.emptyList(), /* jvmArgs */
                "openjdk", /* vm name*/
                "1.8.0", /* jdk version */
                "1.8.0", /* vm version */
                "1.19", /* jmh version */
                TimeValue.NONE /* timeout */ );
    }
}
