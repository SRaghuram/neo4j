/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.Units;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.util.Resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.client.Units.toTimeUnit;

import static java.util.stream.Collectors.toList;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;

public class VarianceProcedure
{
    private static final String BENCHMARKS_FOR_GROUP =
            Resources.fileToString( "/queries/read/benchmarks_for_group_and_series.cypher" );
    private static final String BENCHMARK_POINTS_FOR_SERIES =
            Resources.fileToString( "/queries/read/benchmark_points_for_series.cypher" );

    @Context
    public GraphDatabaseService db;

    @UserFunction( name = "bench.varianceForBenchmark" )
    public Map<String,Double> varianceForBenchmark(
            @Name( "groupNode" ) Node groupNode,
            @Name( "benchmarkNode" ) Node benchmarkNode,
            @Name( "neo4jSeries" ) String neo4jSeries,
            @Name( "owner" ) String branchOwner )
    {
        BenchmarkGroup group = new BenchmarkGroup( (String) groupNode.getProperty( BenchmarkGroup.NAME ) );
        Map<String,String> params = benchmarkNode
                .getSingleRelationship( withName( "HAS_PARAMS" ), OUTGOING )
                .getEndNode()
                .getAllProperties().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> (String) e.getValue() ) );
        Benchmark benchmark = Benchmark.benchmarkFor( (String) benchmarkNode.getProperty( Benchmark.DESCRIPTION ),
                                                      (String) benchmarkNode.getProperty( Benchmark.SIMPLE_NAME ),
                                                      Benchmark.Mode.valueOf( (String) benchmarkNode.getProperty( Benchmark.MODE ) ),
                                                      params );
        BenchmarkVariance benchmarkVariance = variancesFor(
                group,
                newArrayList( benchmark ),
                neo4jSeries,
                branchOwner ).get( 0 );
        Map<String,Double> diffsHist = new HashMap<>();
        IntStream.range( 0, 101 )
                 .forEach( i -> diffsHist.put(
                         Integer.toString( i ),
                         benchmarkVariance.variance().diffAtPercentile( i )
                                             ) );
        return diffsHist;
    }

    @Procedure( name = "bench.variancesForGroup", mode = Mode.READ )
    public Stream<BenchmarkVarianceRow> variancesForGroup(
            @Name( "groupNode" ) Node groupNode,
            @Name( "neo4jSeries" ) String neo4jSeries,
            @Name( "owner" ) String branchOwner )
    {
        BenchmarkGroup group = new BenchmarkGroup( (String) groupNode.getProperty( BenchmarkGroup.NAME ) );
        List<Benchmark> benchmarks = benchmarksFor( group.name(), neo4jSeries, branchOwner );
        List<BenchmarkVariance> benchmarkVariances = variancesFor( group, benchmarks, neo4jSeries, branchOwner );
        return benchmarkVariances.stream().map( BenchmarkVarianceRow::new );
    }

    @Procedure( name = "bench.variancesForBenchmarks", mode = Mode.READ )
    public Stream<BenchmarkVarianceRow> variancesForBenchmarks(
            @Name( "groupNode" ) Node groupNode,
            @Name( "benchmarkNodes" ) List<Node> benchmarkNodes,
            @Name( "neo4jSeries" ) String neo4jSeries,
            @Name( "owner" ) String branchOwner )
    {
        BenchmarkGroup group = new BenchmarkGroup( (String) groupNode.getProperty( BenchmarkGroup.NAME ) );
        List<Benchmark> benchmarks = benchmarkNodes.stream()
                                                   .map( b ->
                                                         {
                                                             Map<String,String> params = b
                                                                     .getSingleRelationship( withName( "HAS_PARAMS" ), OUTGOING )
                                                                     .getEndNode()
                                                                     .getAllProperties().entrySet().stream()
                                                                     .collect( Collectors.toMap( Map.Entry::getKey, e -> (String) e.getValue() ) );
                                                             return Benchmark.benchmarkFor( (String) b.getProperty( Benchmark.DESCRIPTION ),
                                                                                            (String) b.getProperty( Benchmark.SIMPLE_NAME ),
                                                                                            (String) b.getProperty( Benchmark.NAME ),
                                                                                            Benchmark.Mode.valueOf( (String) b.getProperty( Benchmark.MODE ) ),
                                                                                            params );
                                                         } )
                                                   .collect( toList() );
        List<BenchmarkVariance> benchmarkVariances = variancesFor( group, benchmarks, neo4jSeries, branchOwner );
        return benchmarkVariances.stream().map( BenchmarkVarianceRow::new );
    }

    public static class BenchmarkVarianceRow
    {
        public final String group;
        public final String benchmark;
        public final String unit;
        public final String mode;
        public final double mean;
        public final List<Double> points;
        public final Map<String,Double> diffsHist;
        public final List<Double> diffs;

        BenchmarkVarianceRow( BenchmarkVariance benchmarkVariance )
        {
            this.group = benchmarkVariance.benchmarkGroup().name();
            this.benchmark = benchmarkVariance.benchmark().name();
            this.unit = Units.toAbbreviation( benchmarkVariance.variance().unit() );
            this.mode = benchmarkVariance.benchmark().mode().toString();
            this.mean = benchmarkVariance.variance().mean();
            this.points = benchmarkVariance.series().points( Point.BY_DATE ).stream()
                                           .map( Point::value )
                                           .collect( toList() );
            this.diffsHist = new HashMap<>();
            IntStream.range( 0, 101 )
                     .forEach( i -> this.diffsHist.put(
                             Integer.toString( i ),
                             benchmarkVariance.variance().diffAtPercentile( i )
                                                      ) );
            this.diffs = Arrays.stream( benchmarkVariance.variance().diffs() ).boxed().collect( toList() );
        }
    }

    private List<BenchmarkVariance> variancesFor(
            BenchmarkGroup group,
            List<Benchmark> benchmarks,
            String neo4jSeries,
            String branchOwner )
    {
        List<BenchmarkVariance> benchmarkVariances = new ArrayList<>();
        for ( Benchmark benchmark : benchmarks )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "group_name", group.name() );
            params.put( "benchmark_name", benchmark.name() );
            params.put( "neo4j_series", neo4jSeries );
            params.put( "owner", branchOwner );

            List<Point> points = db.execute( BENCHMARK_POINTS_FOR_SERIES, params ).stream()
                                   .map( rowMap ->
                                                 new Point(
                                                         ((Number) rowMap.get( "metricsNodeId" )).longValue(),
                                                         ((Number) rowMap.get( "date" )).longValue(),
                                                         ((Number) rowMap.get( "mean" )).doubleValue(),
                                                         toTimeUnit( (String) rowMap.get( "unit" ) ),
                                                         new Neo4j( ((Node) rowMap.get( "neo4j" )).getAllProperties() ) )
                                       )
                                   .collect( toList() );

            Series series = new Series( neo4jSeries, points, benchmark.mode() );
            Variance variance = Variance.calculateFor( series );
            BenchmarkVariance benchmarkVariance = new BenchmarkVariance(
                    group,
                    benchmark,
                    series,
                    variance );
            benchmarkVariances.add( benchmarkVariance );
        }
        benchmarkVariances.sort( BenchmarkVariance.BY_VALUE );
        return benchmarkVariances;
    }

    private List<Benchmark> benchmarksFor( String groupName, String neo4jSeries, String branchOwner )
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "group", groupName );
        params.put( "neo4j_series", neo4jSeries );
        params.put( "owner", branchOwner );
        return db.execute( BENCHMARKS_FOR_GROUP, params ).stream()
                 .map( rowMap ->
                               Benchmark.benchmarkFor( (String) ((Node) rowMap.get( "b" )).getProperty( Benchmark.DESCRIPTION ),
                                                       (String) ((Node) rowMap.get( "b" )).getProperty( Benchmark.SIMPLE_NAME ),
                                                       (String) ((Node) rowMap.get( "b" )).getProperty( Benchmark.NAME ),
                                                       Benchmark.Mode.valueOf( (String) ((Node) rowMap.get( "b" )).getProperty( Benchmark.MODE ) ),
                                                       toMap( (Node) rowMap.get( "bp" ) ) )
                     )
                 .collect( toList() );
    }

    private Map<String,String> toMap( Node node )
    {
        return node.getAllProperties().keySet().stream()
                   .collect( Collectors.toMap( k -> k, k -> (String) node.getProperty( k ) ) );
    }
}
