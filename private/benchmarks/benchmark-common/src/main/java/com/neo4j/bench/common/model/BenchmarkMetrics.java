/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.model.Benchmark.Mode;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

public class BenchmarkMetrics
{
    private final Benchmark benchmark;
    private final Metrics metrics;

    public static BenchmarkMetrics extractBenchmarkMetrics( Map<String,Object> benchmarkMap,
                                                            Map<String,Object> metricsMap,
                                                            Map<String,Object> benchmarkParamsMap )
    {
        return new BenchmarkMetrics(
                (String) benchmarkMap.get( Benchmark.SIMPLE_NAME ),
                (String) benchmarkMap.get( Benchmark.DESCRIPTION ),
                Benchmark.Mode.valueOf( (String) benchmarkMap.get( Benchmark.MODE ) ),
                benchmarkParamsMap.entrySet().stream().collect( toMap( Map.Entry::getKey, e -> e.getValue().toString() ) ),
                metricsMap );
    }

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public BenchmarkMetrics()
    {
        this( "1", "", Mode.LATENCY, emptyMap(), new Metrics().toMap() );
    }

    public BenchmarkMetrics(
            String simpleName,
            String description,
            Mode mode,
            Map<String,String> parameters,
            Map<String,Object> metricsMap )
    {
        this.benchmark = Benchmark.benchmarkFor( description, simpleName, mode, parameters );
        this.metrics = Metrics.fromMap( metricsMap );
    }

    public Benchmark benchmark()
    {
        return benchmark;
    }

    public Metrics metrics()
    {
        return metrics;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkMetrics that = (BenchmarkMetrics) o;
        return Objects.equals( benchmark, that.benchmark ) &&
               Objects.equals( metrics, that.metrics );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( benchmark, metrics );
    }

    @Override
    public String toString()
    {
        return "BenchmarkMetrics{benchmark=" + benchmark + ", metrics=" + metrics + "}";
    }
}
