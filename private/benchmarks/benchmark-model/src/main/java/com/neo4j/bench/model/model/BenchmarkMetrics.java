/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

public class BenchmarkMetrics
{
    private final Benchmark benchmark;
    private final Metrics metrics;
    private final Metrics maybeAuxiliaryMetrics;

    public static BenchmarkMetrics extractBenchmarkMetrics( Map<String,Object> benchmarkMap,
                                                            Map<String,Object> metricsMap,
                                                            Map<String,Object> maybeAuxiliaryMetricsMap,
                                                            Map<String,Object> benchmarkParamsMap )
    {
        return new BenchmarkMetrics(
                (String) benchmarkMap.get( Benchmark.SIMPLE_NAME ),
                (String) benchmarkMap.get( Benchmark.DESCRIPTION ),
                Benchmark.Mode.valueOf( (String) benchmarkMap.get( Benchmark.MODE ) ),
                benchmarkParamsMap.entrySet().stream().collect( toMap( Map.Entry::getKey, e -> e.getValue().toString() ) ),
                metricsMap,
                maybeAuxiliaryMetricsMap );
    }

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public BenchmarkMetrics()
    {
        this( "1", "", Benchmark.Mode.LATENCY, emptyMap(), new Metrics().toMap(), new Metrics().toMap() );
    }

    public BenchmarkMetrics(
            String simpleName,
            String description,
            Benchmark.Mode mode,
            Map<String,String> parameters,
            Map<String,Object> metricsMap,
            Map<String,Object> maybeAuxiliaryMetricsMap )
    {
        this.benchmark = Benchmark.benchmarkFor( description, simpleName, mode, parameters );
        this.metrics = Metrics.fromMap( metricsMap );
        this.maybeAuxiliaryMetrics = null == maybeAuxiliaryMetricsMap
                                     ? null
                                     : Metrics.fromMap( maybeAuxiliaryMetricsMap );
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return "(" + benchmark + "," + metrics + "," + maybeAuxiliaryMetrics + ")";
    }
}
