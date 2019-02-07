/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.neo4j.bench.micro.benchmarks.test_only.ValidDisabledBenchmark;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark1;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark2;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static java.util.stream.Collectors.toSet;

public class AnnotationsTest
{
    @Test
    public void shouldPassValidation()
    {
        Annotations annotations = new Annotations();
        Annotations.AnnotationsValidationResult validationResult = annotations.validate();

        assertTrue( validationResult.message(), validationResult.isValid() );
    }

    @Test
    public void shouldReadDescription()
    {
        String description = Annotations.descriptionFor( ValidEnabledBenchmark1.class );

        assertThat( description, equalTo( ValidEnabledBenchmark1.class.getSimpleName() ) );
    }

    @Test
    public void shouldReadParameterFields()
    {
        Set<String> parameters = Annotations.paramFieldsFor( ValidEnabledBenchmark1.class )
                .map( Field::getName )
                .collect( toSet() );

        assertThat(
                parameters,
                equalTo( newHashSet( "ValidEnabledBenchmark1_number", "ValidEnabledBenchmark1_string" ) ) );
    }

    @Test
    public void shouldReadIsEnabled()
    {
        boolean benchmark1IsEnabled = Annotations.isEnabled( ValidEnabledBenchmark1.class );
        boolean benchmark2IsEnabled = Annotations.isEnabled( ValidEnabledBenchmark2.class );
        boolean benchmark3IsEnabled = Annotations.isEnabled( ValidDisabledBenchmark.class );

        assertTrue( benchmark1IsEnabled );
        assertTrue( benchmark2IsEnabled );
        assertFalse( benchmark3IsEnabled );
    }

    @Test
    public void shouldReadBenchmarkGroup()
    {
        String group = Annotations.benchmarkGroupFor( ValidEnabledBenchmark1.class );

        assertThat( group, equalTo( "Example" ) );
    }

    @Test
    public void shouldReadBenchmarkMethods()
    {
        Set<String> benchmarkMethodNames = Annotations.benchmarkMethodsFor( ValidEnabledBenchmark1.class )
                .map( Method::getName )
                .collect( toSet() );

        assertThat( benchmarkMethodNames, equalTo( newHashSet( "methodOne", "methodTwo" ) ) );
    }

    @Test
    public void shouldReadBenchmarkIsThreadSafe()
    {
        boolean benchmark1IsThreadSafe = Annotations.isThreadSafe( ValidEnabledBenchmark1.class );
        boolean benchmark2IsThreadSafe = Annotations.isThreadSafe( ValidEnabledBenchmark2.class );

        assertFalse( benchmark1IsThreadSafe );
        assertTrue( benchmark2IsThreadSafe );
    }
}
