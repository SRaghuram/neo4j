/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.Benchmark.Mode;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Metrics.MetricsUnit;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.model.model.Benchmark.Mode.ACCURACY;
import static com.neo4j.bench.model.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.model.model.Benchmark.Mode.SINGLE_SHOT;
import static com.neo4j.bench.model.model.Benchmark.Mode.THROUGHPUT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BenchmarkGroupBenchmarkMetricsPrinterTest
{
    private static final MetricsUnit TIME_UNIT = MetricsUnit.latency( TimeUnit.MILLISECONDS );
    private static final MetricsUnit ACCURACY_UNIT = MetricsUnit.accuracy();
    public static final BenchmarkGroup GROUP_1 = new BenchmarkGroup( "group1" );

    private static Metrics metricsAccuracy( MetricsUnit unit )
    {
        return new Metrics( unit,
                            0.1D,
                            357.9D,
                            2.06D,
                            54_345L,
                            0.79D,
                            1.95D,
                            8.17D,
                            9.89D,
                            145.567D,
                            207.1D,
                            356.874621D );
    }

    private static Metrics metricsSmall( MetricsUnit unit )
    {
        return new Metrics( unit,
                            1D,
                            10D,
                            4D,
                            42L,
                            2D,
                            5D,
                            7D,
                            9D,
                            9.4D,
                            9.89D,
                            9.997D );
    }

    private static Metrics metricsBig( MetricsUnit unit )
    {
        return new Metrics( unit,
                            1D,
                            1_000_000_000D,
                            400_000_000D,
                            4_200_000_000L,
                            200_000_000D,
                            500_000_000D,
                            700_000_000D,
                            900_000_000D,
                            940_000_000D,
                            990_000_000D,
                            999_000_000D );
    }

    public static Benchmark benchmark1( Mode mode )
    {
        return Benchmark.benchmarkFor( "description",
                                       "name1",
                                       mode,
                                       ImmutableMap.<String,String>builder()
                                               .put( "p1a", "v1a" )
                                               .put( "p1b", "v1b" )
                                               .build() );
    }

    public static Benchmark benchmark2( Mode mode )
    {
        return Benchmark.benchmarkFor( "description",
                                       "name2",
                                       mode,
                                       ImmutableMap.<String,String>builder()
                                               .put( "p2a", "v2a" )
                                               .put( "p2b", "v2b" )
                                               .build() );
    }

    @Test
    public void shouldPrintSimpleLatencyMetrics()
    {
        BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
        results.add( GROUP_1, benchmark1( LATENCY ), metricsSmall( TIME_UNIT ), null, Neo4jConfig.empty() );
        results.add( GROUP_1, benchmark2( LATENCY ), metricsBig( TIME_UNIT ), null, Neo4jConfig.empty() );

        BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );

        String expected =
                "----------------------------------------------------------------------------------------------------------------------\n" +
                "Group   Benchmark                                           Count        Mean  Min   Median     90th        Max   Unit\n" +
                "----------------------------------------------------------------------------------------------------------------------\n" +
                "group1  name1_(p1a,v1a)_(p1b,v1b)_(mode,LATENCY)               42        4.00    1        5        9         10  ms/op\n" +
                "group1  name2_(p2a,v2a)_(p2b,v2b)_(mode,LATENCY)    4,200,000,000  400,000.00    0  500,000  900,000  1,000,000   s/op\n" +
                "----------------------------------------------------------------------------------------------------------------------\n";
        assertThat( printer.toPrettyString( results ), equalTo( expected ) );
    }

    @Test
    public void shouldPrintSimpleThroughputMetrics()
    {
        BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
        results.add( GROUP_1, benchmark1( THROUGHPUT ), metricsSmall( TIME_UNIT ), null, Neo4jConfig.empty() );
        results.add( GROUP_1, benchmark2( THROUGHPUT ), metricsBig( TIME_UNIT ), null, Neo4jConfig.empty() );

        BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );

        String expected =
                "-------------------------------------------------------------------------------------------------------------\n" +
                "Group   Benchmark                                              Count    Mean  Min  Median  90th    Max   Unit\n" +
                "-------------------------------------------------------------------------------------------------------------\n" +
                "group1  name1_(p1a,v1a)_(p1b,v1b)_(mode,THROUGHPUT)               42    4.00    1       5     9     10  op/ms\n" +
                "group1  name2_(p2a,v2a)_(p2b,v2b)_(mode,THROUGHPUT)    4,200,000,000  400.00    0     500   900  1,000  op/ns\n" +
                "-------------------------------------------------------------------------------------------------------------\n";
        assertThat( printer.toPrettyString( results ), equalTo( expected ) );
    }

    @Test
    public void shouldPrintSimpleSingleShotMetrics()
    {
        BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
        results.add( GROUP_1, benchmark1( SINGLE_SHOT ), metricsSmall( TIME_UNIT ), null, Neo4jConfig.empty() );
        results.add( GROUP_1, benchmark2( SINGLE_SHOT ), metricsBig( TIME_UNIT ), null, Neo4jConfig.empty() );

        BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );

        String expected =
                "--------------------------------------------------------------------------------------------------------------------------\n" +
                "Group   Benchmark                                               Count        Mean  Min   Median     90th        Max   Unit\n" +
                "--------------------------------------------------------------------------------------------------------------------------\n" +
                "group1  name1_(p1a,v1a)_(p1b,v1b)_(mode,SINGLE_SHOT)               42        4.00    1        5        9         10  ms/op\n" +
                "group1  name2_(p2a,v2a)_(p2b,v2b)_(mode,SINGLE_SHOT)    4,200,000,000  400,000.00    0  500,000  900,000  1,000,000   s/op\n" +
                "--------------------------------------------------------------------------------------------------------------------------\n";
        assertThat( printer.toPrettyString( results ), equalTo( expected ) );
    }

    @Test
    public void shouldPrintSimpleAccuracyMetrics()
    {
        BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
        results.add( GROUP_1, benchmark1( ACCURACY ), metricsAccuracy( ACCURACY_UNIT ), null, Neo4jConfig.empty() );
        results.add( GROUP_1, benchmark2( ACCURACY ), metricsAccuracy( ACCURACY_UNIT ), null, Neo4jConfig.empty() );

        BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );

        String expected =
                "---------------------------------------------------------------------------------------------------\n" +
                "Group   Benchmark                                     Count  Mean  Min  Median  90th  Max      Unit\n" +
                "---------------------------------------------------------------------------------------------------\n" +
                "group1  name1_(p1a,v1a)_(p1b,v1b)_(mode,ACCURACY)    54,345  2.06    0       2    10  358  accuracy\n" +
                "group1  name2_(p2a,v2a)_(p2b,v2b)_(mode,ACCURACY)    54,345  2.06    0       2    10  358  accuracy\n" +
                "---------------------------------------------------------------------------------------------------\n";
        assertThat( printer.toPrettyString( results ), equalTo( expected ) );
    }

    @Test
    public void shouldPrintLatencyAndRowMetrics()
    {
        BenchmarkGroupBenchmarkMetrics results = new BenchmarkGroupBenchmarkMetrics();
        results.add( GROUP_1, benchmark1( LATENCY ), metricsSmall( TIME_UNIT ), metricsSmall( MetricsUnit.rows() ), Neo4jConfig.empty() );
        results.add( GROUP_1, benchmark2( LATENCY ), metricsBig( TIME_UNIT ), metricsBig( MetricsUnit.rows() ), Neo4jConfig.empty() );

        BenchmarkGroupBenchmarkMetricsPrinter printer = new BenchmarkGroupBenchmarkMetricsPrinter( true );

        String expected =
                "--------------------------------------------------------------------------------------------------------------------------------------\n" +
                "Group   Benchmark                                           Count            Mean  Min       Median         90th            Max   Unit\n" +
                "--------------------------------------------------------------------------------------------------------------------------------------\n" +
                "group1  name1_(p1a,v1a)_(p1b,v1b)_(mode,LATENCY)               42            4.00    1            5            9             10  ms/op\n" +
                "                                                                             4.00    1            5            9             10   rows\n" +
                "group1  name2_(p2a,v2a)_(p2b,v2b)_(mode,LATENCY)    4,200,000,000      400,000.00    0      500,000      900,000      1,000,000   s/op\n" +
                "                                                                   400,000,000.00    1  500,000,000  900,000,000  1,000,000,000   rows\n" +
                "--------------------------------------------------------------------------------------------------------------------------------------\n";
        assertThat( printer.toPrettyString( results ), equalTo( expected ) );
    }
}
