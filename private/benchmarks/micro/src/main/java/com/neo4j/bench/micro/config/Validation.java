/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_BENCHMARK_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_PARAMETER_DOES_NOT_EXIST;
import static com.neo4j.bench.micro.config.Validation.ValidationError.CONFIGURED_VALUE_IS_NOT_ALLOWED;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_ALLOWED_VALUE;
import static com.neo4j.bench.micro.config.Validation.ValidationError.DUPLICATE_BASE_VALUE;
import static com.neo4j.bench.micro.config.Validation.ValidationError.PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK;
import static com.neo4j.bench.micro.config.Validation.ValidationError.PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES;
import static com.neo4j.bench.micro.config.Validation.ValidationError.UNRECOGNIZED_CONFIG_FILE_ENTRY;

import static java.lang.String.format;

/**
 * The Validation object gathers validation errors as they occur during reflective benchmark gathering, config
 * loading and config application. isValid() checks whether any validation errors occurred, and report() gives a
 * textual summary of the encountered errors, grouped by type.
 */
public class Validation
{
    public enum ValidationError
    {
        PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK,
        UNRECOGNIZED_CONFIG_FILE_ENTRY,
        PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES,
        CONFIGURED_BENCHMARK_DOES_NOT_EXIST,
        CONFIGURED_PARAMETER_DOES_NOT_EXIST,
        CONFIGURED_VALUE_IS_NOT_ALLOWED,
        DUPLICATE_ALLOWED_VALUE,
        DUPLICATE_BASE_VALUE
    }

    private final List<String> paramConfiguredWithoutEnablingDisablingBenchmarkErrs = new ArrayList<>();
    private final List<String> unrecognizedConfigFileEntryErrs = new ArrayList<>();
    private final List<String> paramOfEnabledBenchmarkConfiguredWithNoValuesErrs = new ArrayList<>();
    private final List<String> configuredBenchmarkDoesNotExistErrs = new ArrayList<>();
    private final List<String> configuredParameterDoesNotExistErrs = new ArrayList<>();
    private final List<String> configuredValueIsNotAllowedErrs = new ArrayList<>();
    private final List<String> duplicateAllowedValueErrs = new ArrayList<>();
    private final List<String> duplicateBaseValueErrs = new ArrayList<>();
    private final Set<ValidationError> validationErrors = new HashSet<>();

    public static void assertValid( Validation validation )
    {
        if ( !validation.isValid() )
        {
            throw new RuntimeException( validation.report() );
        }
    }

    void duplicateAllowedValue( String benchmarkName, String paramName, String[] allowed )
    {
        duplicateAllowedValueErrs.add( format( "%s.%s, allowed = %s",
                benchmarkName, paramName, Arrays.toString( allowed ) ) );
        validationErrors.add( DUPLICATE_ALLOWED_VALUE );
    }

    void duplicateBaseValue( String benchmarkName, String paramName, String[] base )
    {
        duplicateBaseValueErrs.add( format( "%s.%s, base = %s", benchmarkName, paramName, Arrays.toString( base ) ) );
        validationErrors.add( DUPLICATE_BASE_VALUE );
    }

    void unrecognizedConfigFileEntry( String configFileEntry )
    {
        unrecognizedConfigFileEntryErrs.add( configFileEntry );
        validationErrors.add( UNRECOGNIZED_CONFIG_FILE_ENTRY );
    }

    void configuredBenchmarkDoesNotExist( String benchmarkName )
    {
        configuredBenchmarkDoesNotExistErrs.add( benchmarkName );
        validationErrors.add( CONFIGURED_BENCHMARK_DOES_NOT_EXIST );
    }

    void configuredParameterDoesNotExist( String benchmarkName, String paramName )
    {
        configuredParameterDoesNotExistErrs.add( format( "%s.%s", benchmarkName, paramName ) );
        validationErrors.add( CONFIGURED_PARAMETER_DOES_NOT_EXIST );
    }

    void paramConfiguredWithoutEnablingDisablingBenchmark( String benchmarkName, String paramName )
    {
        paramConfiguredWithoutEnablingDisablingBenchmarkErrs.add( format( "%s.%s", benchmarkName, paramName ) );
        validationErrors.add( PARAM_CONFIGURED_WITHOUT_ENABLING_DISABLING_BENCHMARK );
    }

    void paramOfEnabledBenchmarkConfiguredWithNoValues( String benchmarkName, String paramName )
    {
        paramOfEnabledBenchmarkConfiguredWithNoValuesErrs.add( format( "%s.%s = []", benchmarkName, paramName ) );
        validationErrors.add( PARAM_OF_ENABLED_BENCHMARK_CONFIGURED_WITH_NO_VALUES );
    }

    void configuredValueIsNotAllowed( String benchmarkName, String paramName, Set<String> allowedValues, String value )
    {
        configuredValueIsNotAllowedErrs.add(
                format( "%s.%s = %s ALLOWED: %s",
                        benchmarkName, paramName, value, Arrays.toString( allowedValues.toArray() ) ) );
        validationErrors.add( CONFIGURED_VALUE_IS_NOT_ALLOWED );
    }

    public boolean errorsEqual( ValidationError... errors )
    {
        return newHashSet( errors ).equals( validationErrors );
    }

    public boolean isValid()
    {
        return validationErrors.isEmpty();
    }

    public String report()
    {
        if ( isValid() )
        {
            return "Validation Passed";
        }
        else
        {
            StringBuilder sb = new StringBuilder( "Validation Failed\n" );
            if ( !paramConfiguredWithoutEnablingDisablingBenchmarkErrs.isEmpty() )
            {
                sb.append( "\tBenchmark parameter configured without enabling/disabling corresponding benchmark:\n" );
                appendErrors( sb, paramConfiguredWithoutEnablingDisablingBenchmarkErrs );
            }
            if ( !paramOfEnabledBenchmarkConfiguredWithNoValuesErrs.isEmpty() )
            {
                sb.append( "\tParameter of enabled benchmark configured with no values:\n" );
                appendErrors( sb, paramOfEnabledBenchmarkConfiguredWithNoValuesErrs );
            }
            if ( !configuredBenchmarkDoesNotExistErrs.isEmpty() )
            {
                sb.append( "\tConfigured benchmark does not exist:\n" );
                appendErrors( sb, configuredBenchmarkDoesNotExistErrs );
            }
            if ( !configuredParameterDoesNotExistErrs.isEmpty() )
            {
                sb.append( "\tConfigured parameter does not exist:\n" );
                appendErrors( sb, configuredParameterDoesNotExistErrs );
            }
            if ( !configuredValueIsNotAllowedErrs.isEmpty() )
            {
                sb.append( "\tConfigured value is not allowed:\n" );
                appendErrors( sb, configuredValueIsNotAllowedErrs );
            }
            if ( !duplicateAllowedValueErrs.isEmpty() )
            {
                sb.append( "\tSettings have duplicate allowed values:\n" );
                appendErrors( sb, duplicateAllowedValueErrs );
            }
            if ( !duplicateBaseValueErrs.isEmpty() )
            {
                sb.append( "\tSettings have duplicate base values:\n" );
                appendErrors( sb, duplicateBaseValueErrs );
            }
            if ( !unrecognizedConfigFileEntryErrs.isEmpty() )
            {
                sb.append( "\tUnrecognized configuration file entries:\n" );
                appendErrors( sb, unrecognizedConfigFileEntryErrs );
            }
            return sb.toString();
        }
    }

    private void appendErrors( StringBuilder sb, List<String> errors )
    {
        errors.forEach( err -> sb.append( "\t\t" ).append( err ).append( "\n" ) );
    }
}
