/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.neo4j.bench.jmh.api.benchmarks.test_only.ValidEnabledBenchmark1;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AnnotationsTest extends AnnotationsFixture
{
    @Test
    public void shouldPassValidation()
    {
        Annotations annotations = new Annotations( getPackageName() );
        Annotations.AnnotationsValidationResult validationResult = annotations.validate();

        assertTrue( validationResult.message(), validationResult.isValid() );
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
    public void shouldReadBenchmarkMethods()
    {
        Set<String> benchmarkMethodNames = new Annotations( getPackageName() )
                .getBenchmarkMethodsFor( ValidEnabledBenchmark1.class )
                .stream()
                .map( Method::getName )
                .collect( toSet() );

        assertThat( benchmarkMethodNames, equalTo( newHashSet( "methodOne", "methodTwo" ) ) );
    }
}
