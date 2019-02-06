/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.queries;

import com.neo4j.bench.client.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.procedures.detection.BenchmarkVariance;
import com.neo4j.bench.procedures.detection.Point;
import com.neo4j.bench.procedures.detection.Series;
import com.neo4j.bench.procedures.detection.Variance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import static com.neo4j.bench.client.Units.toTimeUnit;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class GetBenchmarkVariances implements Query<List<BenchmarkVariance>>
{
    private static final String BENCHMARK_POINTS_FOR_SERIES =
            Resources.fileToString( "/queries/read/benchmark_points_for_series.cypher" );

    private final List<BenchmarkGroupBenchmark> benchmarks;
    private final String neo4jSeries;

    public GetBenchmarkVariances( List<BenchmarkGroupBenchmark> benchmarks, String neo4jSeries )
    {
        this.benchmarks = benchmarks;
        this.neo4jSeries = neo4jSeries;
    }

    @Override
    public List<BenchmarkVariance> execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            List<BenchmarkVariance> benchmarkVariances = new ArrayList<>();
            for ( BenchmarkGroupBenchmark benchmark : benchmarks )
            {
                Map<String,Object> params = new HashMap<>();
                params.put( "benchmark_name", benchmark.benchmark().name() );
                params.put( "neo4j_series", neo4jSeries );
                List<Point> points = session.run( BENCHMARK_POINTS_FOR_SERIES, params ).list().stream()
                                            .map( r ->
                                                          new Point(
                                                                  r.get( "metricsNodeId" ).asLong(),
                                                                  r.get( "date" ).asLong(),
                                                                  r.get( "mean" ).asDouble(),
                                                                  toTimeUnit( r.get( "unit" ).asString() ),
                                                                  new Neo4j( r.get( "neo4j" ) ) )
                                            )
                                            .collect( toList() );
                Series series = new Series( neo4jSeries, points, benchmark.benchmark().mode() );
                Variance variance = Variance.calculateFor( series );
                BenchmarkVariance benchmarkVariance = new BenchmarkVariance(
                        benchmark.benchmarkGroup(),
                        benchmark.benchmark(),
                        series,
                        variance );
                benchmarkVariances.add( benchmarkVariance );
            }
            benchmarkVariances.sort( BenchmarkVariance.BY_VALUE );
            return benchmarkVariances;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieve benchmark variances", e );
        }
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return format( "%s(%s)", getClass().getSimpleName(), neo4jSeries );
    }
}
