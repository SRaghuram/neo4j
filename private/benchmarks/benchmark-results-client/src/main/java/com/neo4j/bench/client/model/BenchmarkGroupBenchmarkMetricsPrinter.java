/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class BenchmarkGroupBenchmarkMetricsPrinter
{
    private static final DecimalFormat MEAN_FORMAT = new DecimalFormat( "###,###,##0.00" );
    private final boolean verbose;

    public BenchmarkGroupBenchmarkMetricsPrinter( boolean verbose )
    {
        this.verbose = verbose;
    }

    public String toPrettyString( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        return toPrettyString( results, errors, verbose );
    }

    public String toPrettyString( BenchmarkGroupBenchmarkMetrics results )
    {
        List<TestRunError> errors = new ArrayList<>();
        return toPrettyString( results, errors, verbose );
    }

    private static String toPrettyString( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors, boolean verbose )
    {
        int longestGroupName = longestStringIn( results.benchmarkGroupBenchmarks().stream()
                                                       .map( BenchmarkGroupBenchmark::benchmarkGroup )
                                                       .map( BenchmarkGroup::name ) );

        int longestBenchmarkName = longestStringIn( results.benchmarkGroupBenchmarks().stream()
                                                           .map( BenchmarkGroupBenchmark::benchmark )
                                                           .map( Benchmark::name ) );

        String format = prettyPrintFormat( verbose, longestGroupName, longestBenchmarkName );

        StringBuilder sb = new StringBuilder();
        String header = prettyHeader( verbose, longestGroupName, longestBenchmarkName );
        sb.append( header ).append( "\n" );
        appendPrettyMetricsString( sb, verbose, results, format );

        for ( TestRunError error : errors )
        {
            sb.append( prettyErrorRow( verbose, error.groupName(), error.benchmarkName(), format ) ).append( "\n" );
        }

        return sb.toString();
    }

    private static String prettyHeader( boolean verbose, int longestGroupName, int longestBenchmarkName )
    {
        String format = prettyPrintFormat( verbose, longestGroupName, longestBenchmarkName );
        return verbose
               ? String.format( format, "Group", "Benchmark", "Count", "Mean", "Min", "Median", "90th", "Max", "Unit" )
               : String.format( format, "Group", "Benchmark", "Count", "Mean", "Unit" );
    }

    private static void appendPrettyMetricsString( StringBuilder sb, boolean verbose, BenchmarkGroupBenchmarkMetrics results, String format )
    {
        for ( BenchmarkGroupBenchmark benchmark : results.benchmarkGroupBenchmarks().stream()
                                                         .sorted( new BenchmarkGroupBenchmarkComparator() )
                                                         .collect( toList() ) )
        {
            Metrics metrics = results.getMetricsFor( benchmark ).metrics();
            sb.append( prettyMetricsRow( verbose, benchmark.benchmarkGroup(), benchmark.benchmark(), metrics, format ) ).append( "\n" );
        }
    }

    private static String prettyMetricsRow( boolean verbose,
                                            BenchmarkGroup group,
                                            Benchmark benchmark,
                                            Metrics metrics,
                                            String format )
    {
        return verbose
               ? String.format( format,
                                group.name(),
                                benchmark.name(),
                                metrics.toMap().get( Metrics.SAMPLE_SIZE ),
                                MEAN_FORMAT.format( metrics.toMap().get( Metrics.MEAN ) ),
                                ((Number) metrics.toMap().get( Metrics.MIN )).longValue(),
                                ((Number) metrics.toMap().get( Metrics.PERCENTILE_50 )).longValue(),
                                ((Number) metrics.toMap().get( Metrics.PERCENTILE_90 )).longValue(),
                                ((Number) metrics.toMap().get( Metrics.MAX )).longValue(),
                                metrics.toMap().get( Metrics.UNIT ) )
               : String.format( format,
                                group.name(),
                                benchmark.name(),
                                metrics.toMap().get( Metrics.SAMPLE_SIZE ),
                                MEAN_FORMAT.format( metrics.toMap().get( Metrics.MEAN ) ),
                                metrics.toMap().get( Metrics.UNIT ) );
    }

    private static String prettyErrorRow( boolean verbose,
                                          String group,
                                          String benchmark,
                                          String format )
    {
        return verbose
               ? String.format( format,
                                group,
                                benchmark,
                                "<error>",
                                "<error>",
                                "<error>",
                                "<error>",
                                "<error>",
                                "<error>",
                                "<error>" )
               : String.format( format,
                                group,
                                benchmark,
                                "<error>",
                                "<error>",
                                "<error>" );
    }

    private static int longestStringIn( Stream<String> strings )
    {
        return strings.mapToInt( String::length ).max().orElse( 0 );
    }

    private static String prettyPrintFormat( boolean verbose, int longestGroupName, int longestBenchmarkName )
    {
        return verbose
               ? "%1$-" + longestGroupName + "s   %2$-" + longestBenchmarkName + "s   %3$-10s %4$-10s %5$-10s %6$-10s %7$-10s %8$-10s %9$-10s"
               : "%1$-" + longestGroupName + "s   %2$-" + longestBenchmarkName + "s   %3$-10s %4$-10s %5$-10s";
    }

    private static class BenchmarkGroupBenchmarkComparator implements Comparator<BenchmarkGroupBenchmark>
    {
        @Override
        public int compare( BenchmarkGroupBenchmark o1, BenchmarkGroupBenchmark o2 )
        {
            int groupCompare = o1.benchmarkGroup().name().compareTo( o2.benchmarkGroup().name() );
            if ( 0 != groupCompare )
            {
                return groupCompare;
            }
            else
            {
                return o1.benchmark().name().compareTo( o2.benchmark().name() );
            }
        }
    }
}
