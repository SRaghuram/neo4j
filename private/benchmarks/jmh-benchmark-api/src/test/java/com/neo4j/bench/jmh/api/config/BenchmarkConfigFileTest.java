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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.jmh.api.config.BenchmarkConfigFile.fromMap;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK;
import static com.neo4j.bench.jmh.api.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BenchmarkConfigFileTest extends BenchmarksFinderFixture
{
    // READ

    @Test
    public void shouldEnableBenchmark()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName = ValidDisabledBenchmark.class.getName();
        BenchmarkConfigFile benchmarkConfigFile = fromMap( map( benchmarkName, "true" ), validation, getBenchmarksFinder() );

        // then
        assertThat( benchmarkConfigFile.entries().size(), equalTo( 1 ) );
        assertTrue( benchmarkConfigFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName ).values().size(), equalTo( 0 ) );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldNotIgnoreDisabledBenchmarks()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        BenchmarkConfigFile benchmarkConfigFile =
                fromMap( map( benchmarkName, "false" ), validation, getBenchmarksFinder() );

        // then
        assertThat( benchmarkConfigFile.entries().size(), equalTo( 1 ) );
        assertFalse( benchmarkConfigFile.getEntry( benchmarkName ).isEnabled() );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldParseParam()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        String paramName = "number";
        String paramValue = "2";
        BenchmarkConfigFile benchmarkConfigFile =
                fromMap( map( benchmarkName, "true", benchmarkName + "." + paramName, paramValue ), validation, getBenchmarksFinder() );

        // then
        assertThat( benchmarkConfigFile.entries().size(), equalTo( 1 ) );
        assertTrue( benchmarkConfigFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName ).values().size(), equalTo( 1 ) );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName ).values().get( paramName ),
                    equalTo( newHashSet( paramValue ) ) );

        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldParseInvalidParam()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        String paramName = "invalid";
        String paramValue = "irrelevant";
        BenchmarkConfigFile benchmarkConfigFile = fromMap(
                map( benchmarkName, "true", benchmarkName + "." + paramName, paramValue ),
                validation,
                getBenchmarksFinder() );

        // then
        assertThat( benchmarkConfigFile.entries().size(), equalTo( 1 ) );
        assertTrue( benchmarkConfigFile.getEntry( benchmarkName ).isEnabled() );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName ).values().size(), equalTo( 1 ) );
        assertThat( validation.report(),
                    benchmarkConfigFile.getEntry( benchmarkName ).values().get( paramName ),
                    equalTo( newHashSet( paramValue ) ) );

        assertTrue( validation.isValid() );
    }

    @Test
    public void shouldParseTwoBenchmarks()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName1 = ValidEnabledBenchmark1.class.getName();
        String benchmarkName2 = ValidDisabledBenchmark.class.getName();
        BenchmarkConfigFile benchmarkConfigFile =
                fromMap( map( benchmarkName1, "true", benchmarkName2, "true" ), validation, getBenchmarksFinder() );

        // then
        assertThat( benchmarkConfigFile.entries().size(), equalTo( 2 ) );
        assertTrue( benchmarkConfigFile.getEntry( benchmarkName1 ).isEnabled() );
        assertTrue( benchmarkConfigFile.getEntry( benchmarkName2 ).isEnabled() );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName1 ), notNullValue() );
        assertThat( benchmarkConfigFile.getEntry( benchmarkName2 ), notNullValue() );

        assertTrue( validation.report(), validation.errors().isEmpty() );
        assertTrue( validation.report(), validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenEmptyParam()
    {
        // when
        Validation validation = new Validation();
        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        String paramName = "number";
        fromMap( map( benchmarkName, "true", benchmarkName + "." + paramName, "" ), validation, getBenchmarksFinder() );

        // then
        assertEquals( validation.report(), validation.errors(), singleton( PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES ) );
        assertFalse( validation.isValid() );
    }

    @Test
    public void shouldFailValidationWhenParamConfiguredButBenchmarkNotExplicitlyEnabledDisabled()
    {
        // when
        Validation validation = new Validation();

        String benchmarkName = ValidEnabledBenchmark1.class.getName();
        String paramName = "number";
        fromMap( map( benchmarkName + "." + paramName, "1" ), validation, getBenchmarksFinder() );

        // then
        assertEquals( validation.report(), validation.errors(), singleton( PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK ) );
        assertFalse( validation.isValid() );
    }

    // WRITE

    @Test
    public void shouldWriteBenchmark()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );

        String serializedConf = BenchmarkConfigFile.toString(
                suiteDescription,
                newHashSet( ValidEnabledBenchmark1.class.getName() ),
                false,
                false );

        assertThat( serializedConf, equalTo(
                "# Benchmark: enable/disable\n" +
                ValidEnabledBenchmark1.class.getName() + " = true\n\n" ) );
    }

    @Test
    public void shouldWriteTwoBenchmarks()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );

        String serializedConf = BenchmarkConfigFile.toString(
                suiteDescription,
                newHashSet( ValidEnabledBenchmark1.class.getName(), ValidDisabledBenchmark.class.getName() ),
                false,
                false );

        assertThat( serializedConf,
                    containsString( ValidEnabledBenchmark1.class.getName() + " = true\n\n" ) );
        assertThat( serializedConf,
                    containsString( ValidDisabledBenchmark.class.getName() + " = true\n\n" ) );
        assertThat( serializedConf, not( containsString( "false" ) ) );
    }

    @Test
    public void shouldWriteVerboseBenchmark()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );

        String serializedConf = BenchmarkConfigFile.toString(
                suiteDescription,
                newHashSet( ValidEnabledBenchmark1.class.getName() ),
                true,
                false );

        assertThat( serializedConf, equalTo(
                "# Benchmark: enable/disable\n" +
                ValidEnabledBenchmark1.class.getName() + " = true\n" +
                "# -----\n" +
                "# JMH Param: number\n" +
                "# Valid: 1, 2\n" +
                ValidEnabledBenchmark1.class.getName() + ".number = 1\n" +
                "# -----\n" +
                "# JMH Param: string\n" +
                "# Valid: a, b\n" +
                ValidEnabledBenchmark1.class.getName() + ".string = a, b\n\n"
        ) );
    }

    @Test
    public void shouldWriteDisabledBenchmarks()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );

        Set<String> enabled = newHashSet( ValidEnabledBenchmark1.class.getName() );
        String serializedConf = BenchmarkConfigFile.toString(
                suiteDescription,
                enabled,
                false,
                true );

        for ( BenchmarkDescription benchDesc : suiteDescription.benchmarks() )
        {
            assertThat( serializedConf,
                        containsString( benchDesc.className() + " = " + enabled.contains( benchDesc.className() ) ) );
        }
    }

    @Test
    public void shouldWriteVerboseDisabledBenchmark()
    {
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getBenchmarksFinder(), new Validation() );

        String serializedConf = BenchmarkConfigFile.toString(
                suiteDescription,
                new HashSet<>(),
                true,
                true );

        assertThat( serializedConf,
                    containsString(
                            "# Benchmark: enable/disable\n" +
                            ValidEnabledBenchmark1.class.getName() + " = false\n" +
                            "# -----\n" +
                            "# JMH Param: number\n" +
                            "# Valid: 1, 2\n" +
                            ValidEnabledBenchmark1.class.getName() + ".number = 1\n" ) );
    }

    // HELPERS

    private Map<String,String> map( String... keyValues )
    {
        assert keyValues.length % 2 == 0;
        Map<String,String> theMap = new HashMap<>();
        for ( int i = 0; i < keyValues.length; i += 2 )
        {
            theMap.put( keyValues[i], keyValues[i + 1] );
        }
        return theMap;
    }
}
