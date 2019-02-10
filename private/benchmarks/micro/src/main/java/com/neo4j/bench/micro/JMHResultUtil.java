/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.Benchmark.Mode;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Benchmarks;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.micro.config.Annotations;
import com.neo4j.bench.micro.config.JmhOptionsUtil;
import com.neo4j.bench.micro.config.ParameterValue;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.util.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.micro.config.Annotations.descriptionFor;
import static com.neo4j.bench.micro.config.BenchmarkDescription.simplifyParamName;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class JMHResultUtil
{
    private static final double ERROR_CONFIDENCE = 0.999;
    static final String THREADS_PARAM = "threads";
    static final String MODE_PARAM = "mode";

    public static BenchmarkGroup toBenchmarkGroup( BenchmarkParams benchmarkParams )
    {
        String benchmarkMethodName = benchmarkParams.getBenchmark();
        String benchmarkClassName = benchmarkMethodName.substring( 0, benchmarkMethodName.lastIndexOf( "." ) );
        String benchmarkGroup = Annotations.benchmarkGroupFor( benchmarkClassName );
        return new BenchmarkGroup( benchmarkGroup );
    }

    public static Benchmarks toBenchmarks( BenchmarkParams benchmarkParams )
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
        List<ParameterValue> parameterValues = extractParameterValues( benchmarkParams );
        String parameterizedNameSuffix = toNameSuffix( parameterValues, mode );

        List<String> parameterizedBenchmarkNames = simpleBenchmarkNames.stream()
                .map( name -> name + parameterizedNameSuffix )
                .collect( toList() );

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
                statistics,
                ERROR_CONFIDENCE );
    }

    private static String withoutPackageName( String benchmarkName )
    {
        int classNameMethodNameSeparatorIndex = benchmarkName.lastIndexOf( "." );
        int packageClassSeparatorIndex = benchmarkName.lastIndexOf( ".", classNameMethodNameSeparatorIndex - 1 );
        return benchmarkName.substring( packageClassSeparatorIndex + 1 );
    }

    private static String withoutMethodName( String benchmarkName )
    {
        int classNameMethodNameSeparatorIndex = benchmarkName.lastIndexOf( "." );
        return benchmarkName.substring( 0, classNameMethodNameSeparatorIndex );
    }

    static List<ParameterValue> extractParameterValues( BenchmarkParams benchmarkParams )
    {
        List<ParameterValue> parameterValues = benchmarkParams.getParamsKeys().stream()
                .filter( param -> !JmhOptionsUtil.GLOBAL_PARAMS.contains( param ) )
                .map( param -> new ParameterValue( simplifyParamName( param ), benchmarkParams.getParam( param ) ) )
                .collect( toList() );
        parameterValues.add( new ParameterValue( THREADS_PARAM, Integer.toString( benchmarkParams.getThreads() ) ) );
        return parameterValues;
    }

    // orders parameters to achieve determinism, names are used as keys in results store
    static String toNameSuffix( List<ParameterValue> parameterValues, Mode mode )
    {
        Collections.sort( parameterValues );
        return parameterValues.stream()
                .filter( parameterValue -> !JmhOptionsUtil.GLOBAL_PARAMS.contains( parameterValue.param() ) )
                .map( parameterValue -> format( "_(%s,%s)", parameterValue.param(), parameterValue.value() ) )
                .reduce( "", ( name, parameterValueString ) -> name + parameterValueString )
                .concat( format( "_(%s,%s)", MODE_PARAM, mode.name() ) );
    }

    private static Metrics toMetrics(
            TimeUnit timeUnit,
            Statistics statistics,
            double errorConfidence )
    {
        return new Metrics(
                timeUnit,
                statistics.getMin(),
                statistics.getMax(),
                statistics.getMean(),
                statistics.getMeanErrorAt( errorConfidence ),
                errorConfidence,
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
    private static Mode toNativeMode( org.openjdk.jmh.annotations.Mode mode )
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
}
