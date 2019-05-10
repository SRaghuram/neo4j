/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.neo4j.bench.jmh.api.benchmarks.valid.ValidDisabledBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SuiteDescriptionTest extends AnnotationsFixture
{
    @Test
    public void shouldFailValidationWhenOnlySomeParametersAreSet()
    {
        // when
        String benchmarkName = ValidDisabledBenchmark.class.getName();

        Map<String,Set<String>> values = new HashMap<>();
        values.put( "param1", newHashSet( "1", "2" ) );
        values.put( "param2", newHashSet() );

        BenchmarkConfigFile configFile = new BenchmarkConfigFile( singletonMap(
                benchmarkName,
                new BenchmarkConfigFileEntry(
                        benchmarkName,
                        true,
                        values ) ) );

        Validation validation = new Validation();

        // then
        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName ).values().size(), equalTo( 2 ) );

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        SuiteDescription.fromConfig( suiteDescription, configFile, validation );

        assertTrue( validation.report(),
                    validation.errorsEqual( PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES ) );
        assertFalse( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenProvidedWithInvalidParam()
    {
        // when
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        String paramName = "invalid";
        Set<String> paramValues = newHashSet( "irrelevant" );

        BenchmarkConfigFile configFile = new BenchmarkConfigFile( singletonMap(
                benchmarkName,
                new BenchmarkConfigFileEntry(
                        benchmarkName,
                        true,
                        singletonMap( paramName, paramValues ) ) ) );

        Validation validation = new Validation();

        // then
        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName ).values().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).values().containsKey( paramName ) );

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        SuiteDescription.fromConfig( suiteDescription, configFile, validation );

        assertTrue( validation.report(),
                    validation.errorsEqual( CONFIGURED_PARAMETER_DOES_NOT_EXIST ) );
        assertFalse( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldReturnCorrectGroupBenchmarkNames()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), new Validation() );
        Map<String,List<String>> groupBenchmarks = suiteDescription.getGroupBenchmarkNames();
        // contains 'Example' group only
        assertThat( groupBenchmarks.toString(), groupBenchmarks.size(), equalTo( 1 ) );

        List<String> exampleBenchmarks = groupBenchmarks.get( "Example" );
        assertThat( exampleBenchmarks.size(), equalTo( 9 ) );
        getAnnotations().getBenchmarks().forEach(
                benchmark -> assertTrue( exampleBenchmarks.contains( benchmark.getName() ) ) );
    }
}
