/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.neo4j.bench.micro.benchmarks.test_only.ValidDisabledBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark1;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark2;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.micro.TestUtils.map;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SuiteDescriptionTest
{
    private SuiteDescription suiteDescription;

    @Before
    public void setup()
    {
        Validation validation = new Validation();
        suiteDescription = SuiteDescription.byReflection( validation );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenOnlySomeParametersAreSet()
    {
        // when
        String benchmarkName = ValidDisabledBenchmark.class.getName();

        BenchmarkConfigFile configFile = new BenchmarkConfigFile( map(
                benchmarkName,
                new BenchmarkConfigFileEntry(
                        benchmarkName,
                        true,
                        map( "param1", newHashSet( "1", "2" ), "param2", newHashSet() ) ) ) );

        Validation validation = new Validation();

        // then
        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName ).values().size(), equalTo( 2 ) );

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

        BenchmarkConfigFile configFile = new BenchmarkConfigFile( map(
                benchmarkName,
                new BenchmarkConfigFileEntry(
                        benchmarkName,
                        true,
                        map( paramName, paramValues ) ) ) );

        Validation validation = new Validation();

        // then
        assertThat( configFile.entries().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( configFile.getEntry( benchmarkName ).values().size(), equalTo( 1 ) );
        assertTrue( configFile.getEntry( benchmarkName ).values().containsKey( paramName ) );

        SuiteDescription.fromConfig( suiteDescription, configFile, validation );

        assertTrue( validation.report(),
                validation.errorsEqual( CONFIGURED_PARAMETER_DOES_NOT_EXIST ) );
        assertFalse( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldReturnCorrectGroupBenchmarkNames()
    {
        Map<String,List<String>> groupBenchmarks = suiteDescription.getGroupBenchmarkNames();
        // contains at least 'Example' and one other (e.g., 'Core API')
        assertThat( groupBenchmarks.size(), greaterThanOrEqualTo( 2 ) );

        List<String> exampleBenchmarks = groupBenchmarks.get( "Example" );
        assertThat( exampleBenchmarks.size(), equalTo( 3 ) );
        assertTrue( exampleBenchmarks.contains( ValidDisabledBenchmark.class.getName() ) );
        assertTrue( exampleBenchmarks.contains( ValidEnabledBenchmark1.class.getName() ) );
        assertTrue( exampleBenchmarks.contains( ValidEnabledBenchmark2.class.getName() ) );
    }
}
