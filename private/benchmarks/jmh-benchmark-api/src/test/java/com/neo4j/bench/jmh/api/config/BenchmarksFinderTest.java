/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.jmh.api.benchmarks.invalid.DuplicateAllowedBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.invalid.DuplicateBaseBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.invalid.WithParamValuesBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.invalid.WithoutModeBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidDisabledBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark2;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmarkWithoutParams;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledGroupBenchmark;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BenchmarksFinderTest extends BenchmarksFinderFixture
{
    private Set<Class> invalidBenchmarks = Sets.newHashSet( DuplicateAllowedBenchmark.class,
                                                            DuplicateBaseBenchmark.class,
                                                            WithoutModeBenchmark.class,
                                                            WithParamValuesBenchmark.class );

    private Set<Class> validBenchmarks = Sets.newHashSet( ValidDisabledBenchmark.class,
                                                          ValidEnabledBenchmark1.class,
                                                          ValidEnabledBenchmark2.class,
                                                          ValidEnabledBenchmarkWithoutParams.class,
                                                          ValidEnabledGroupBenchmark.class );

    @Test
    public void shouldGetParameterFieldsForBenchmark()
    {
        BenchmarksFinder benchmarksFinder = getBenchmarksFinder();
        List<Field> fields = benchmarksFinder.getParamFieldsFor( ValidDisabledBenchmark.class );
        Set<String> fieldNames = fields.stream().map( Field::getName ).collect( toSet() );
        assertThat( fieldNames, equalTo( Sets.newHashSet( "ValidDisabledBenchmark_param1", "ValidDisabledBenchmark_param2" ) ) );
    }

    @Test
    public void shouldGetParameterFieldsForBenchmarksWithoutParameters()
    {
        BenchmarksFinder benchmarksFinder = getBenchmarksFinder();
        List<Field> fields = benchmarksFinder.getParamFieldsFor( ValidEnabledBenchmarkWithoutParams.class );
        assertTrue( fields.isEmpty() );
    }

    @Test
    public void shouldGetAllBenchmarks()
    {
        assertThat( getBenchmarksFinder().getBenchmarks(), equalTo( Sets.union( invalidBenchmarks, validBenchmarks ) ) );
        assertThat( getInvalidBenchmarksFinder().getBenchmarks(), equalTo( invalidBenchmarks ) );
        assertThat( getValidBenchmarksFinder().getBenchmarks(), equalTo( validBenchmarks ) );
    }

    @Test
    public void shouldHasBenchmark()
    {
        for ( Class benchmark : invalidBenchmarks )
        {
            assertTrue( getBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
            assertTrue( getInvalidBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
            assertFalse( getValidBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
        }
        for ( Class benchmark : validBenchmarks )
        {
            assertTrue( getBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
            assertFalse( getInvalidBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
            assertTrue( getValidBenchmarksFinder().hasBenchmark( benchmark.getName() ) );
        }
    }

    @Test
    public void shouldGetBenchmarkMethods()
    {
        BenchmarksFinder benchmarksFinder = getBenchmarksFinder();

        // group benchmark
        List<String> groupBenchmarkMethods =
                benchmarksFinder.getBenchmarkMethodsFor( ValidEnabledGroupBenchmark.class ).stream()
                                .map( benchmarksFinder::benchmarkNameFor )
                                .collect( toList() );
        assertThat( groupBenchmarkMethods, equalTo( Lists.newArrayList( "group", "group" ) ) );

        // regular benchmark
        Set<String> benchmarkMethods =
                benchmarksFinder.getBenchmarkMethodsFor( ValidEnabledBenchmark1.class ).stream()
                                .map( benchmarksFinder::benchmarkNameFor )
                                .collect( toSet() );
        assertThat( benchmarkMethods, equalTo( Sets.newHashSet( "methodOne", "methodTwo" ) ) );
    }

    @Test
    public void shouldPassValidationWhenAllBenchmarksAreValid()
    {
        BenchmarksFinder benchmarksFinder = getValidBenchmarksFinder();
        BenchmarksValidator.BenchmarkValidationResult validationResult = benchmarksFinder.validate();

        assertTrue( validationResult.message(), validationResult.isValid() );
    }

    @Test
    public void shouldFailValidationWhenSomeBenchmarksAreInvalid()
    {
        BenchmarksFinder benchmarksFinder = getBenchmarksFinder();
        BenchmarksValidator.BenchmarkValidationResult validationResult = benchmarksFinder.validate();

        assertFalse( validationResult.isValid() );
    }

    @Test
    public void shouldReadParameterFields()
    {
        Set<String> parameters = getBenchmarksFinder().getParamFieldsFor( ValidEnabledBenchmark1.class ).stream()
                                                      .map( Field::getName )
                                                      .collect( toSet() );

        assertThat(
                parameters,
                equalTo( newHashSet( "ValidEnabledBenchmark1_number", "ValidEnabledBenchmark1_string" ) ) );
    }

    @Test
    public void shouldReadBenchmarkMethods()
    {
        Set<String> benchmarkMethodNames = getBenchmarksFinder()
                .getBenchmarkMethodsFor( ValidEnabledBenchmark1.class )
                .stream()
                .map( Method::getName )
                .collect( toSet() );

        assertThat( benchmarkMethodNames, equalTo( newHashSet( "methodOne", "methodTwo" ) ) );
    }
}
