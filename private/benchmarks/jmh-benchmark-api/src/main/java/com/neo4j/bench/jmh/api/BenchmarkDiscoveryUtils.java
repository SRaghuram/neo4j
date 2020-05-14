/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.google.common.collect.Lists;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Benchmarks;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParameterValue;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.util.Statistics;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class BenchmarkDiscoveryUtils
{
    static final String THREADS_PARAM = "threads";

    // -------------------------- JMH-to-Benchmark Bridge Utilities --------------------------

    public static BenchmarkGroup toBenchmarkGroup( BenchmarkParams benchmarkParams )
    {
        String benchmarkMethodName = benchmarkParams.getBenchmark();
        String benchmarkClassName = benchmarkMethodName.substring( 0, benchmarkMethodName.lastIndexOf( '.' ) );
        String benchmarkGroup = benchmarkGroupFor( benchmarkClassName );
        return new BenchmarkGroup( benchmarkGroup );
    }

    public static Benchmarks toBenchmarks( BenchmarkParams benchmarkParams, RunnerParams runnerParams )
    {
        // all benchmark methods in @Group have the same mode
        Mode mode = toNativeMode( benchmarkParams.getMode() );

        // all benchmark methods in @Group have the same description
        String benchmarkDescription = descriptionFor( withoutMethodName( benchmarkParams.getBenchmark() ) );

        // for 'symmetric' (non-@Group) benchmarks simple name will be class + method
        // for 'asymmetric' (@Group) benchmarks simple name will be class + group
        String simpleBenchmarkName = withoutPackageName( benchmarkParams.getBenchmark() );
        List<String> simpleBenchmarkNames = Lists.newArrayList( simpleBenchmarkName );
        // thread group labels will be empty for 'symmetric' (non-@Group) benchmarks
        benchmarkParams.getThreadGroupLabels().stream()
                       .map( label -> simpleBenchmarkName + ":" + label )
                       .forEach( simpleBenchmarkNames::add );

        // all benchmark methods for a given class (regardless of @Group/non-@Group) have the same parameters
        List<ParameterValue> parameterValues = extractParameterValues( benchmarkParams, runnerParams );

        Benchmarks benchmarks = new Benchmarks(
                Benchmark.benchmarkFor( benchmarkDescription,
                                        simpleBenchmarkNames.get( 0 ),
                                        mode,
                                        parametersAsMap( parameterValues ) ) );
        for ( int i = 1; i < simpleBenchmarkNames.size(); i++ )
        {
            benchmarks.addChildBenchmark(
                    Benchmark.benchmarkFor( benchmarkDescription,
                                            simpleBenchmarkNames.get( i ),
                                            mode,
                                            parametersAsMap( parameterValues ) ) );
        }
        return benchmarks;
    }

    static Map<String,String> parametersAsMap( List<ParameterValue> parameterValues )
    {
        return assertNoDuplicates( requireNonNull( parameterValues ) ).stream()
                                                                      .collect( toMap( ParameterValue::param, ParameterValue::value ) );
    }

    private static List<ParameterValue> assertNoDuplicates( List<ParameterValue> parameterValues )
    {
        Set<String> parameters = parameterValues.stream().map( ParameterValue::param ).collect( toSet() );
        if ( parameterValues.size() != parameters.size() )
        {
            throw new RuntimeException( format( "Parameters values contained duplicates: %s", parameterValues ) );
        }
        return parameterValues;
    }

    static Metrics toMetrics( Statistics statistics, TimeUnit timeUnit )
    {
        return toMetrics(
                timeUnit,
                statistics );
    }

    private static String withoutPackageName( String benchmarkName )
    {
        int classNameMethodNameSeparatorIndex = benchmarkName.lastIndexOf( '.' );
        int packageClassSeparatorIndex = benchmarkName.lastIndexOf( '.', classNameMethodNameSeparatorIndex - 1 );
        return benchmarkName.substring( packageClassSeparatorIndex + 1 );
    }

    private static String withoutMethodName( String benchmarkName )
    {
        int classNameMethodNameSeparatorIndex = benchmarkName.lastIndexOf( '.' );
        return benchmarkName.substring( 0, classNameMethodNameSeparatorIndex );
    }

    static List<ParameterValue> extractParameterValues( BenchmarkParams benchmarkParams, RunnerParams runnerParams )
    {
        List<ParameterValue> parameterValues = benchmarkParams.getParamsKeys().stream()
                                                              // exclude runner parameters from benchmark name
                                                              .filter( paramName -> !runnerParams.containsParam( paramName ) )
                                                              .map( param -> new ParameterValue( param, benchmarkParams.getParam( param ) ) )
                                                              .collect( toList() );
        parameterValues.add( new ParameterValue( THREADS_PARAM, Integer.toString( benchmarkParams.getThreads() ) ) );
        return parameterValues;
    }

    private static Metrics toMetrics(
            TimeUnit timeUnit,
            Statistics statistics )
    {
        return new Metrics(
                timeUnit,
                statistics.getMin(),
                statistics.getMax(),
                statistics.getMean(),
                statistics.getN(),
                statistics.getPercentile( 25 ),
                statistics.getPercentile( 50 ),
                statistics.getPercentile( 75 ),
                statistics.getPercentile( 90 ),
                statistics.getPercentile( 95 ),
                statistics.getPercentile( 99 ),
                statistics.getPercentile( 99.9 ) );
    }

    // do own to string to protect against JMH changing naming, which would affect the contents stored results
    public static Mode toNativeMode( org.openjdk.jmh.annotations.Mode mode )
    {
        switch ( mode )
        {
        case Throughput:
            return Mode.THROUGHPUT;
        case AverageTime:
        case SampleTime:
            return Mode.LATENCY;
        case SingleShotTime:
            return Mode.SINGLE_SHOT;
        default:
            throw new RuntimeException( "Unrecognized mode: " + mode );
        }
    }

    // -------------------------- Benchmark Discovery Utilities --------------------------

    public static String benchmarkGroupFor( String benchmarkClassName )
    {
        return benchmarkGroupFor( benchmarkClassForName( benchmarkClassName ) );
    }

    public static String benchmarkGroupFor( Class<? extends BaseBenchmark> benchmarkClass )
    {
        try
        {
            BaseBenchmark abstractBenchmark = benchmarkClass.getDeclaredConstructor().newInstance();
            return abstractBenchmark.benchmarkGroup();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'benchmark group' from: " + benchmarkClass.getName(), e );
        }
    }

    public static String descriptionFor( String benchmarkClassName )
    {
        return descriptionFor( benchmarkClassForName( benchmarkClassName ) );
    }

    public static String descriptionFor( Class<? extends BaseBenchmark> benchmarkClass )
    {
        try
        {
            BaseBenchmark abstractBenchmark = benchmarkClass.getDeclaredConstructor().newInstance();
            return abstractBenchmark.description();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'description' from: " + benchmarkClass.getName(), e );
        }
    }

    public static boolean isEnabled( Class benchmarkClass )
    {
        return !benchmarkClass.isAnnotationPresent( BenchmarkEnabled.class ) ||
               ((BenchmarkEnabled) benchmarkClass.getAnnotation( BenchmarkEnabled.class )).value();
    }

    public static boolean isThreadSafe( Class<? extends BaseBenchmark> benchmarkClass )
    {
        try
        {
            BaseBenchmark abstractBenchmark = benchmarkClass.getDeclaredConstructor().newInstance();
            return abstractBenchmark.isThreadSafe();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving 'thread safe' from: " + benchmarkClass.getName(), e );
        }
    }

    public static Class benchmarkClassForName( String benchmarkClassName )
    {
        try
        {
            Class benchmarkClass = Class.forName( benchmarkClassName );
            if ( !BaseBenchmark.class.isAssignableFrom( benchmarkClass ) )
            {
                throw new RuntimeException( format( "Class with name %s is not a benchmark", benchmarkClassName ) );
            }
            return benchmarkClass;
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( "Unable to get class for: " + benchmarkClassName );
        }
    }
}
