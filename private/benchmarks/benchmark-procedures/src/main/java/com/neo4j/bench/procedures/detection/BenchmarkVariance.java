package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Benchmark.Mode;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.client.Units.toAbbreviation;

import static java.lang.String.format;

public class BenchmarkVariance
{
    public static Comparator<BenchmarkVariance> BY_VALUE = new VarianceComparator();
    private final BenchmarkGroup benchmarkGroup;
    private final Benchmark benchmark;
    private final Series series;
    private final Variance variance;

    public BenchmarkVariance( BenchmarkGroup benchmarkGroup, Benchmark benchmark, Series series, Variance variance )
    {
        this.benchmarkGroup = benchmarkGroup;
        this.benchmark = benchmark;
        this.series = series;
        this.variance = variance;
    }

    BenchmarkGroup benchmarkGroup()
    {
        return benchmarkGroup;
    }

    Benchmark benchmark()
    {
        return benchmark;
    }

    Series series()
    {
        return series;
    }

    TimeUnit unit()
    {
        return variance.unit();
    }

    Mode mode()
    {
        return benchmark.mode();
    }

    Variance variance()
    {
        return variance;
    }

    private static class VarianceComparator implements Comparator<BenchmarkVariance>
    {
        @Override
        public int compare( BenchmarkVariance o1, BenchmarkVariance o2 )
        {
            int median = Double.compare(
                    o2.variance().diffAtPercentile( 50 ),
                    o1.variance().diffAtPercentile( 50 ) );
            if ( 0 == median )
            {
                int percentile = Double.compare(
                        o2.variance().diffAtPercentile( 75 ),
                        o1.variance().diffAtPercentile( 75 ) );
                if ( 0 == percentile )
                {
                    return Double.compare(
                            o2.variance().diffAtPercentile( 100 ),
                            o1.variance().diffAtPercentile( 100 ) );
                }
                return percentile;
            }
            return median;
        }
    }

    @Override
    public String toString()
    {

        String unit = toAbbreviation( unit(), mode() );
        return format( "%s\n\t%s\n\t%s\n\t%s - %s",
                       getClass().getSimpleName(), benchmarkGroup.name(), benchmark.name(), unit, variance );
    }
}
