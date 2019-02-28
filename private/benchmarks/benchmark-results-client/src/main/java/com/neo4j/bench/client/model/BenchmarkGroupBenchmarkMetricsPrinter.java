/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class BenchmarkGroupBenchmarkMetricsPrinter
{
    private final boolean verbose;

    public BenchmarkGroupBenchmarkMetricsPrinter( boolean verbose )
    {
        this.verbose = verbose;
    }

    public String toPrettyString( BenchmarkGroupBenchmarkMetrics results )
    {
        return toPrettyString( results, Collections.emptyList() );
    }

    public String toPrettyString( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        assertNotEmpty( results, errors );
        int longestGroupName = longestGroup( results, errors );
        int longestBenchmarkName = longestBenchmark( results, errors );

        RowWriter rowWriter = verbose
                              ? new VerboseRowWriter( longestGroupName, longestBenchmarkName )
                              : new ConciseRowWriter( longestGroupName, longestBenchmarkName );

        StringBuilder sb = new StringBuilder();
        sb.append( rowWriter.prettyHeader() ).append( "\n" );

        for ( BenchmarkGroupBenchmark benchmark : results.benchmarkGroupBenchmarks().stream()
                                                         .sorted( new BenchmarkGroupBenchmarkComparator() )
                                                         .collect( toList() ) )
        {
            Metrics metrics = results.getMetricsFor( benchmark ).metrics();
            sb.append( rowWriter.prettyDataRow( benchmark.benchmarkGroup().name(), benchmark.benchmark().name(), metrics ) ).append( "\n" );
        }

        for ( TestRunError error : errors )
        {
            sb.append( rowWriter.prettyErrorRow( error.groupName(), error.benchmarkName() ) ).append( "\n" );
        }

        return sb.toString();
    }

    private void assertNotEmpty( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        if ( results.benchmarkGroupBenchmarks().isEmpty() && errors.isEmpty() )
        {
            // TODO added as sanity check for now, in reality this should never happen
            throw new RuntimeException( "No results or errors to print!" );
        }
    }

    private int longestGroup( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        return Math.max( longestStringIn( results.benchmarkGroupBenchmarks().stream()
                                                 .map( BenchmarkGroupBenchmark::benchmarkGroup )
                                                 .map( BenchmarkGroup::name ) ),
                         longestStringIn( errors.stream().map( TestRunError::groupName ) ) );
    }

    private int longestBenchmark( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        return Math.max( longestStringIn( results.benchmarkGroupBenchmarks().stream()
                                                 .map( BenchmarkGroupBenchmark::benchmark )
                                                 .map( Benchmark::name ) ),
                         longestStringIn( errors.stream().map( TestRunError::benchmarkName ) ) );
    }

    private static int longestStringIn( Stream<String> strings )
    {
        return strings.mapToInt( String::length ).max().orElse( 0 );
    }

    private static final DecimalFormat MEAN_FORMAT = new DecimalFormat( "###,###,##0.00" );

    private interface RowWriter
    {
        String prettyHeader();

        String prettyDataRow( String group, String benchmark, Metrics metrics );

        String prettyErrorRow( String group, String benchmark );
    }

    private static class ConciseRowWriter implements RowWriter
    {
        private final String format;

        private ConciseRowWriter( int longestGroupName, int longestBenchmarkName )
        {
            this.format = "%1$-" + longestGroupName + "s   %2$-" + longestBenchmarkName + "s   %3$-10s %4$-10s %5$-10s";
        }

        @Override
        public String prettyHeader()
        {
            return String.format( format, "Group", "Benchmark", "Count", "Mean", "Unit" );
        }

        @Override
        public String prettyDataRow( String group, String benchmark, Metrics metrics )
        {
            return String.format( format,
                                  group,
                                  benchmark,
                                  metrics.toMap().get( Metrics.SAMPLE_SIZE ),
                                  MEAN_FORMAT.format( metrics.toMap().get( Metrics.MEAN ) ),
                                  metrics.toMap().get( Metrics.UNIT ) );
        }

        @Override
        public String prettyErrorRow( String group, String benchmark )
        {
            return String.format( format,
                                  group,
                                  benchmark,
                                  "<error>",
                                  "<error>",
                                  "<error>" );
        }
    }

    private static class VerboseRowWriter implements RowWriter
    {
        private final String format;

        private VerboseRowWriter( int longestGroupName, int longestBenchmarkName )
        {
            this.format = "%1$-" + longestGroupName + "s   %2$-" + longestBenchmarkName + "s   %3$-10s %4$-10s %5$-10s %6$-10s %7$-10s %8$-10s %9$-10s";
        }

        @Override
        public String prettyHeader()
        {
            return String.format( format, "Group", "Benchmark", "Count", "Mean", "Min", "Median", "90th", "Max", "Unit" );
        }

        @Override
        public String prettyDataRow( String group, String benchmark, Metrics metrics )
        {
            return String.format( format,
                                  group,
                                  benchmark,
                                  metrics.toMap().get( Metrics.SAMPLE_SIZE ),
                                  MEAN_FORMAT.format( metrics.toMap().get( Metrics.MEAN ) ),
                                  ((Number) metrics.toMap().get( Metrics.MIN )).longValue(),
                                  ((Number) metrics.toMap().get( Metrics.PERCENTILE_50 )).longValue(),
                                  ((Number) metrics.toMap().get( Metrics.PERCENTILE_90 )).longValue(),
                                  ((Number) metrics.toMap().get( Metrics.MAX )).longValue(),
                                  metrics.toMap().get( Metrics.UNIT ) );
        }

        @Override
        public String prettyErrorRow( String group, String benchmark )
        {
            return String.format( format,
                                  group,
                                  benchmark,
                                  "<error>",
                                  "<error>",
                                  "<error>",
                                  "<error>",
                                  "<error>",
                                  "<error>",
                                  "<error>" );
        }
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
