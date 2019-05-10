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

public class AnnotationsTest extends AnnotationsFixture
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
        Annotations annotations = getAnnotations();
        List<Field> fields = annotations.getParamFieldsFor( ValidDisabledBenchmark.class );
        Set<String> fieldNames = fields.stream().map( Field::getName ).collect( toSet() );
        assertThat( fieldNames, equalTo( Sets.newHashSet( "ValidDisabledBenchmark_param1", "ValidDisabledBenchmark_param2" ) ) );
    }

    @Test
    public void shouldGetParameterFieldsForBenchmarksWithoutParameters()
    {
        Annotations annotations = getAnnotations();
        List<Field> fields = annotations.getParamFieldsFor( ValidEnabledBenchmarkWithoutParams.class );
        assertTrue( fields.isEmpty() );
    }

    @Test
    public void shouldGetAllBenchmarks()
    {
        assertThat( getAnnotations().getBenchmarks(), equalTo( Sets.union( invalidBenchmarks, validBenchmarks ) ) );
        assertThat( getInvalidAnnotations().getBenchmarks(), equalTo( invalidBenchmarks ) );
        assertThat( getValidAnnotations().getBenchmarks(), equalTo( validBenchmarks ) );
    }

    @Test
    public void shouldHasBenchmark()
    {
        for ( Class benchmark : invalidBenchmarks )
        {
            assertTrue( getAnnotations().hasBenchmark( benchmark.getName() ) );
            assertTrue( getInvalidAnnotations().hasBenchmark( benchmark.getName() ) );
            assertFalse( getValidAnnotations().hasBenchmark( benchmark.getName() ) );
        }
        for ( Class benchmark : validBenchmarks )
        {
            assertTrue( getAnnotations().hasBenchmark( benchmark.getName() ) );
            assertFalse( getInvalidAnnotations().hasBenchmark( benchmark.getName() ) );
            assertTrue( getValidAnnotations().hasBenchmark( benchmark.getName() ) );
        }
    }

    @Test
    public void shouldGetBenchmarkMethods()
    {
        Annotations annotations = getAnnotations();

        // group benchmark
        List<String> groupBenchmarkMethods =
                annotations.getBenchmarkMethodsFor( ValidEnabledGroupBenchmark.class ).stream().map( annotations::benchmarkNameFor ).collect( toList() );
        assertThat( groupBenchmarkMethods, equalTo( Lists.newArrayList( "group", "group" ) ) );

        // regular benchmark
        Set<String> benchmarkMethods =
                annotations.getBenchmarkMethodsFor( ValidEnabledBenchmark1.class ).stream().map( annotations::benchmarkNameFor ).collect( toSet() );
        assertThat( benchmarkMethods, equalTo( Sets.newHashSet( "methodOne", "methodTwo" ) ) );
    }

    @Test
    public void shouldPassValidationWhenAllBenchmarksAreValid()
    {
        Annotations annotations = getValidAnnotations();
        AnnotationsValidator.AnnotationsValidationResult validationResult = annotations.validate();

        assertTrue( validationResult.message(), validationResult.isValid() );
    }

    @Test
    public void shouldFailValidationWhenSomeBenchmarksAreInvalid()
    {
        Annotations annotations = getAnnotations();
        AnnotationsValidator.AnnotationsValidationResult validationResult = annotations.validate();

        assertFalse( validationResult.isValid() );
    }

    @Test
    public void shouldReadParameterFields()
    {
        Set<String> parameters = getAnnotations().getParamFieldsFor( ValidEnabledBenchmark1.class ).stream()
                                                 .map( Field::getName )
                                                 .collect( toSet() );

        assertThat(
                parameters,
                equalTo( newHashSet( "ValidEnabledBenchmark1_number", "ValidEnabledBenchmark1_string" ) ) );
    }

    @Test
    public void shouldReadBenchmarkMethods()
    {
        Set<String> benchmarkMethodNames = getAnnotations()
                .getBenchmarkMethodsFor( ValidEnabledBenchmark1.class )
                .stream()
                .map( Method::getName )
                .collect( toSet() );

        assertThat( benchmarkMethodNames, equalTo( newHashSet( "methodOne", "methodTwo" ) ) );
    }
}
