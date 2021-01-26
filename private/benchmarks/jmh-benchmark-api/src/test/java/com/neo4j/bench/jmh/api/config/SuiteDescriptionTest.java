/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.neo4j.bench.jmh.api.benchmarks.valid.ValidDisabledBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuiteDescriptionTest extends BenchmarksFinderFixture
{
    @Test
    void shouldFailValidationWhenOnlySomeParametersAreSet()
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

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );
        SuiteDescription.fromConfig( suiteDescription, configFile, validation );

        assertThat( validation.report(),
                    validation.errors(),
                    equalTo( singleton( PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES ) ) );
        assertFalse( validation.isValid(), validation.report() );
    }

    @Test
    void shouldFailValidationWhenProvidedWithInvalidParam()
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

        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );
        SuiteDescription.fromConfig( suiteDescription, configFile, validation );

        assertThat( validation.report(),
                    validation.errors(),
                    equalTo( singleton( CONFIGURED_PARAMETER_DOES_NOT_EXIST ) ) );
        assertFalse( validation.isValid(), validation.report() );
    }

    @Test
    void shouldReturnCorrectGroupBenchmarkNames()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );
        Map<String,List<String>> groupBenchmarks = suiteDescription.getGroupBenchmarkNames();
        // contains 'Example' group only
        assertThat( groupBenchmarks.toString(), groupBenchmarks.size(), equalTo( 1 ) );

        List<String> exampleBenchmarks = groupBenchmarks.get( "Example" );
        assertThat( exampleBenchmarks.size(), equalTo( 12 ) );
        getBenchmarksFinder().getBenchmarks().forEach(
                benchmark -> assertTrue( exampleBenchmarks.contains( benchmark.getName() ) ) );
    }

    @Test
    public void shouldPartitionIntoSmallerPartitions()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getValidBenchmarksFinder(), new Validation() );

        int size = suiteDescription.explodeEnabledBenchmarks().size();
        for ( int i = 2; i < size; i++ )
        {
            List<SuiteDescription> partitions = suiteDescription.partition( i );
            assertEquals( i, partitions.size() );
            assertEquals(
                    new HashSet<>( suiteDescription.explodeEnabledBenchmarks() ),
                    new HashSet<>( partitions.stream().flatMap( partition -> partition.explodeEnabledBenchmarks().stream() ).collect( toList() ) )
            );
        }
    }

    @Test
    public void shouldAlwaysPartitionTheSameWay()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getValidBenchmarksFinder(), new Validation() );
        List<SuiteDescription> partitions = suiteDescription.partition( 3 );

        for ( int i = 0; i < 10000; i++ )
        {
            assertEquals( partitions, suiteDescription.partition( 3 ) );
        }
    }

    @Test
    public void shouldCreateFewerPartitionsIfConcurrencyIsTooHigh()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getValidBenchmarksFinder(), new Validation() );
        int maxPartitions = suiteDescription.explodeEnabledBenchmarks().size();

        List<SuiteDescription> partitions = suiteDescription.partition( maxPartitions * 10 );

        assertEquals( maxPartitions, partitions.size() );

        assertEquals(
                new HashSet<>( suiteDescription.explodeEnabledBenchmarks() ),
                new HashSet<>( partitions.stream().flatMap( partition -> partition.explodeEnabledBenchmarks().stream() ).collect( toList() ) )
        );
    }
}
