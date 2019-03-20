/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.neo4j.bench.client.util.BenchmarkUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.neo4j.bench.client.util.BenchmarkUtil.propertiesPathToMap;
import static com.neo4j.bench.client.util.BenchmarkUtil.splitAndTrimCommaSeparatedString;
import static com.neo4j.bench.micro.config.Annotations.benchmarkClasses;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Reads/parses/writes benchmark configuration files for configuring entire benchmark suite
 */
public class BenchmarkConfigFile
{
    private static final Set<String> BENCHMARKS = benchmarkClasses().stream().map( Class::getName ).collect( toSet() );
    private final Map<String,BenchmarkConfigFileEntry> benchmarkConfigFileEntries;

    public BenchmarkConfigFile()
    {
        this( new HashMap<>() );
    }

    BenchmarkConfigFile( Map<String,BenchmarkConfigFileEntry> benchmarkConfigFileEntries )
    {
        this.benchmarkConfigFileEntries = benchmarkConfigFileEntries;
    }

    public Collection<BenchmarkConfigFileEntry> entries()
    {
        return benchmarkConfigFileEntries.values();
    }

    public boolean hasEntry( String benchmarkName )
    {
        return benchmarkConfigFileEntries.containsKey( benchmarkName );
    }

    public BenchmarkConfigFileEntry getEntry( String benchmarkName )
    {
        return benchmarkConfigFileEntries.get( benchmarkName );
    }

    public static BenchmarkConfigFile fromFile( Path path, Validation validation )
    {
        return fromMap( propertiesPathToMap( path ), validation );
    }

    static BenchmarkConfigFile fromMap( Map<String,String> confMap, Validation validation )
    {

        // TODO remove printout
        System.out.println( "BENCHMARKS is " + BENCHMARKS );

        Map<String,BenchmarkConfigFileEntry> benchmarkConfigFileEntries = benchmarks( confMap );
        for ( String key : confMap.keySet() )
        {
            int separator = key.lastIndexOf( "." );
            if ( separator == -1 )
            {
                validation.unrecognizedConfigFileEntry( key );
                // TODO remove printout
                System.out.println( "BENCHMARKS has unrecognized config file entry " + key );
                continue;
            }

            if ( BENCHMARKS.contains( key ) )
            {
                // TODO remove printout
                System.out.println( "BENCHMARKS contain duplicate config file entry " + key );
                continue;
            }

            String benchmarkNamePrefix = key.substring( 0, separator );
            if ( !BENCHMARKS.contains( benchmarkNamePrefix ) )
            {
                // TODO remove printout
                System.out.println( "BENCHMARKS doesnt exist " + key );
                validation.configuredBenchmarkDoesNotExist( key );
                continue;
            }

            String paramNameSuffix = key.substring( separator + 1 );

            // map contains parameter values for a benchmark it does not enable/disable --> disable by default
            if ( !benchmarkConfigFileEntries.containsKey( benchmarkNamePrefix ) )
            {
                validation.paramConfiguredWithoutEnablingDisablingBenchmark( benchmarkNamePrefix, paramNameSuffix );
                // TODO remove printout
                System.out.println( "BENCHMARKS configured without enabling/disabling benchmark " + key );
                continue;
            }

            BenchmarkConfigFileEntry configFileEntry = benchmarkConfigFileEntries.get( benchmarkNamePrefix );
            String valueString = confMap.get( key );
            Set<String> value = splitAndTrimCommaSeparatedString( valueString );
            if ( configFileEntry.isEnabled() && value.isEmpty() )
            {
                validation.paramOfEnabledBenchmarkConfiguredWithNoValues( benchmarkNamePrefix, paramNameSuffix );
                // TODO remove printout
                System.out.println( "BENCHMARKS param has no values " + key );
                continue;
            }

            configFileEntry.values().put( paramNameSuffix, value );
        }
        return new BenchmarkConfigFile( benchmarkConfigFileEntries );
    }

    private static Map<String,BenchmarkConfigFileEntry> benchmarks( Map<String,String> confMap )
    {
        return confMap.keySet().stream()
                .filter( BENCHMARKS::contains )
                .collect( toMap(
                        identity(),
                        name -> new BenchmarkConfigFileEntry( name, Boolean.valueOf( confMap.get( name ) ) ) ) );
    }

    public static void write(
            SuiteDescription suiteDescription,
            Set<String> benchmarksToEnable,
            boolean verbose,
            boolean withDisabled,
            Path file )
    {
        String configurationString = toString( suiteDescription, benchmarksToEnable, verbose, withDisabled );
        BenchmarkUtil.forceRecreateFile( file );
        try
        {
            Files.write( file, configurationString.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static String toString(
            SuiteDescription suiteDescription,
            Set<String> benchmarksToEnable,
            boolean verbose,
            boolean withDisabled )
    {
        StringBuilder sb = new StringBuilder();
        for ( BenchmarkDescription benchmark : suiteDescription.benchmarks() )
        {
            boolean isEnabled = benchmarksToEnable.contains( benchmark.className() );
            if ( isEnabled || withDisabled )
            {
                sb
                        .append( "# Benchmark: enable/disable\n" )
                        .append( benchmark.className() ).append( " = " ).append( isEnabled ).append( "\n" );

                if ( verbose )
                {
                    // Parameter values
                    for ( BenchmarkParamDescription param : benchmark.parameters().values() )
                    {
                        String paramKey = fullParamName( benchmark, param );
                        String paramValue = sortedString( param.values() );
                        String jmhParamName = param.name();
                        String validValues = sortedString( param.allowedValues() );
                        sb
                                .append( "# -----\n" )
                                .append( "# JMH Param: " ).append( jmhParamName ).append( "\n" )
                                .append( "# Valid: " ).append( validValues ).append( "\n" )
                                .append( paramKey ).append( " = " ).append( paramValue ).append( "\n" );
                    }
                }
                sb.append( "\n" );
            }
        }
        return sb.toString();
    }

    private static String fullParamName( BenchmarkDescription benchmark, BenchmarkParamDescription param )
    {
        return format( "%s.%s", benchmark.className(), param.name() );
    }

    private static String sortedString( Set<String> stringSet )
    {
        String[] values = stringSet.toArray( new String[0] );
        Arrays.sort( values );
        return String.join( ", ", values );
    }
}
