package com.neo4j.bench.client.model;

import com.neo4j.bench.client.model.Benchmark.Mode;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public class BenchmarkMetrics
{
    private final Benchmark benchmark;
    private final Metrics metrics;

    // [[benchmark,metrics,params]]
    public static List<BenchmarkMetrics> extractBenchmarkMetricsList( Value value )
    {
        return value.asList( v -> v ).stream()
                    .map( BenchmarkMetrics::extractBenchmarkMetrics )
                    .collect( toList() );
    }

    // [benchmark,metrics,params]
    private static BenchmarkMetrics extractBenchmarkMetrics( Value value )
    {
        int benchmarkIndex = 0;
        int metricsIndex = 1;
        int benchmarkParamsIndex = 2;
        return new BenchmarkMetrics(
                value.get( benchmarkIndex ).get( Benchmark.SIMPLE_NAME ).asString(),
                value.get( benchmarkIndex ).get( Benchmark.DESCRIPTION ).asString(),
                Benchmark.Mode.valueOf( value.get( benchmarkIndex ).get( Benchmark.MODE ).asString() ),
                value.get( benchmarkParamsIndex ).asMap( Value::asString ),
                value.get( metricsIndex ).asMap() );
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
