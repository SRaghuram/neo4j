/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.collect.Sets;
import org.junit.Test;

import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_BENCHMARK_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_VALUE_IS_NOT_ALLOWED;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_ALLOWED_VALUE;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_BASE_VALUE;
import static com.neo4j.bench.micro.config.Validation.ValidationError.PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK;
import static com.neo4j.bench.micro.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static com.neo4j.bench.micro.config.Validation.ValidationError.UNRECOGNIZED_CONFIG_FILE_ENTRY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationTest
{
    @Test
    public void shouldReportIfValid()
    {
        Validation validation = new Validation();
        assertTrue( validation.report(), validation.isValid() );
        assertThat( validation.report(), equalTo( "Validation Passed" ) );
    }

    @Test
    public void shouldReportAllValidationErrors()
    {
        Validation validation = new Validation();
        assertTrue( validation.report(), validation.isValid() );

        validation.paramConfiguredWithoutEnablingDisablingBenchmark( "b0", "p0a" );

        assertTrue( validation.report(),
                validation.errorsEqual( PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK ) );

        validation.paramConfiguredWithoutEnablingDisablingBenchmark( "b0", "p0b" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK ) );

        validation.paramOfEnabledBenchmarkConfiguredWithNoValues( "b1", "p1" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES ) );

        validation.configuredBenchmarkDoesNotExist( "b3" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST ) );

        validation.configuredParameterDoesNotExist( "b4", "b4" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
                CONFIGURED_PARAMETER_DOES_NOT_EXIST ) );

        validation.configuredValueIsNotAllowed( "b5", "p5", Sets.newHashSet( "a", "b" ), "-1" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
                CONFIGURED_PARAMETER_DOES_NOT_EXIST,
                CONFIGURED_VALUE_IS_NOT_ALLOWED ) );

        validation.duplicateAllowedValue( "b6", "p6", new String[]{"a", "a"} );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
                CONFIGURED_PARAMETER_DOES_NOT_EXIST,
                CONFIGURED_VALUE_IS_NOT_ALLOWED,
                DUPLICATE_ALLOWED_VALUE ) );

        validation.duplicateBaseValue( "b7", "p7", new String[]{"b", "b"} );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
                CONFIGURED_PARAMETER_DOES_NOT_EXIST,
                CONFIGURED_VALUE_IS_NOT_ALLOWED,
                DUPLICATE_ALLOWED_VALUE,
                DUPLICATE_BASE_VALUE ) );

        validation.unrecognizedConfigFileEntry( "p8" );

        assertTrue( validation.report(), validation.errorsEqual(
                PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
                PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
                CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
                CONFIGURED_PARAMETER_DOES_NOT_EXIST,
                CONFIGURED_VALUE_IS_NOT_ALLOWED,
                DUPLICATE_ALLOWED_VALUE,
                DUPLICATE_BASE_VALUE,
                UNRECOGNIZED_CONFIG_FILE_ENTRY ) );

        assertFalse( validation.isValid() );

        String report = validation.report();
        assertThat( report, equalTo(
                "Validation Failed\n" +
                "\tBenchmark parameter configured without enabling/disabling corresponding benchmark:\n" +
                "\t\tb0.p0a\n" +
                "\t\tb0.p0b\n" +
                "\tParameter of enabled benchmark configured with no values:\n" +
                "\t\tb1.p1 = []\n" +
                "\tConfigured benchmark does not exist:\n" +
                "\t\tb3\n" +
                "\tConfigured parameter does not exist:\n" +
                "\t\tb4.b4\n" +
                "\tConfigured value is not allowed:\n" +
                "\t\tb5.p5 = -1 ALLOWED: [a, b]\n" +
                "\tSettings have duplicate allowed values:\n" +
                "\t\tb6.p6, allowed = [a, a]\n" +
                "\tSettings have duplicate base values:\n" +
                "\t\tb7.p7, base = [b, b]\n" +
                "\tUnrecognized configuration file entries:\n" +
                "\t\tp8\n" ) );
    }
}
