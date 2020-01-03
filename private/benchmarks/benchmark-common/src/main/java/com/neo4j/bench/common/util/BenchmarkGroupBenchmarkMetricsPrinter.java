/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.model.Metrics;
import com.neo4j.bench.common.model.TestRunError;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

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

        RowWriter rowWriter = verbose ? new VerboseRowWriter() : new ConciseRowWriter();

        List<String[]> rows = new ArrayList<>();
        for ( BenchmarkGroupBenchmark benchmark : results.benchmarkGroupBenchmarks() )
        {
            Metrics metrics = results.getMetricsFor( benchmark ).metrics();
            rows.add( rowWriter.registerDataRow( benchmark.benchmarkGroup(), benchmark.benchmark(), metrics ) );
        }
        for ( TestRunError error : errors )
        {
            rows.add( rowWriter.registerErrorRow( error.groupName(), error.benchmarkName() ) );
        }
        rows.sort( new RowComparator() );

        StringBuilder sb = new StringBuilder();
        sb.append( rowWriter.separator() ).append( "\n" );
        sb.append( rowWriter.prettyHeader() ).append( "\n" );
        sb.append( rowWriter.separator() ).append( "\n" );
        for ( String[] row : rows )
        {
            sb.append( rowWriter.prettyRow( row ) ).append( "\n" );
        }
        sb.append( rowWriter.separator() ).append( "\n" );
        return sb.toString();
    }

    private void assertNotEmpty( BenchmarkGroupBenchmarkMetrics results, List<TestRunError> errors )
    {
        if ( results.benchmarkGroupBenchmarks().isEmpty() && errors.isEmpty() )
        {
            // Just a sanity check, in reality this should never happen -- hence the sanity check
            throw new RuntimeException( "No results or errors to print!" );
        }
    }

    private static final DecimalFormat INT_FORMAT = new DecimalFormat( "###,###,###,##0" );
    private static final DecimalFormat FLT_FORMAT = new DecimalFormat( "###,###,###,##0.00" );
    private static final String ERROR = "---";

    private abstract static class RowWriter
    {
        private final String[] headers = headers();
        private final int[] columnWidths = Arrays.stream( headers ).mapToInt( String::length ).toArray();
        private final int columnPadding = 2;
        private String format;
        private String separator;

        abstract String[] headers();

        /**
         * Will not be called until all rows have been converted to arrays, i.e., after format has been set.
         * Will be called once per data row.
         *
         * @return data row
         */
        abstract String[] createDataRow( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, TimeUnit originalUnit, TimeUnit saneUnit );

        String[] registerErrorRow( String group, String benchmark )
        {
            String[] row = new String[headers.length];
            row[0] = group;
            row[1] = benchmark;
            for ( int i = 2; i < row.length; i++ )
            {
                row[i] = ERROR;
            }
            return updateWidths( row );
        }

        String[] registerDataRow( BenchmarkGroup group, Benchmark benchmark, Metrics metrics )
        {
            TimeUnit unit = Units.toTimeUnit( (String) metrics.toMap().get( Metrics.UNIT ) );

            // compute unit at which mean is in range [1,1000]
            double mean = (double) metrics.toMap().get( Metrics.MEAN );
            TimeUnit saneUnit = Units.findSaneUnit( mean, unit, benchmark.mode(), 1, 1000 );

            String[] row = createDataRow( group, benchmark, metrics, unit, saneUnit );
            return updateWidths( row );
        }

        private String[] updateWidths( String[] row )
        {
            for ( int i = 0; i < row.length; i++ )
            {
                columnWidths[i] = Math.max( columnWidths[i], row[i].length() );
            }
            return row;
        }

        /**
         * Will not be called until all rows have been converted to arrays, i.e., after format has been set.
         * Will be called exactly once.
         *
         * @return formatted headers
         */
        String prettyHeader()
        {
            // Left justify names
            String nameColumnFormats = IntStream.range( 0, 2 )
                                                .mapToObj( i -> "%" + (i + 1) + "$-" + (columnWidths[i] + columnPadding) + "s" )
                                                .collect( joining( "" ) );
            // Right justify everything else
            String resultColumnFormats = IntStream.range( 2, columnWidths.length )
                                                  .mapToObj( i -> "%" + (i + 1) + "$" + (columnWidths[i] + columnPadding) + "s" )
                                                  .collect( joining( "" ) );
            format = nameColumnFormats + resultColumnFormats;
            return format( format, (Object[]) headers );
        }

        String separator()
        {
            if ( separator == null )
            {
                int totalWidth = Arrays.stream( columnWidths )
                                       .map( columnWidth -> columnWidth + columnPadding )
                                       .sum();
                separator = IntStream.range( 0, totalWidth ).mapToObj( i -> "-" ).collect( joining( "" ) );
            }
            return separator;
        }

        String prettyRow( String[] row )
        {
            return format( format, (Object[]) row );
        }
    }

    private static class ConciseRowWriter extends RowWriter
    {
        @Override
        String[] headers()
        {
            return new String[]{"Group", "Benchmark", "Count", "Mean", "Unit"};
        }

        @Override
        public String[] createDataRow( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, TimeUnit unit, TimeUnit saneUnit )
        {
            return new String[]{
                    group.name(),
                    benchmark.name(),
                    INT_FORMAT.format( metrics.toMap().get( Metrics.SAMPLE_SIZE ) ),
                    FLT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.MEAN ), unit, saneUnit, benchmark.mode() ) ),
                    Units.toAbbreviation( saneUnit, benchmark.mode() )};
        }
    }

    private static class VerboseRowWriter extends RowWriter
    {
        @Override
        String[] headers()
        {
            return new String[]{"Group", "Benchmark", "Count", "Mean", "Min", "Median", "90th", "Max", "Unit"};
        }

        @Override
        String[] createDataRow( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, TimeUnit unit, TimeUnit saneUnit )
        {
            return new String[]{
                    group.name(),
                    benchmark.name(),
                    INT_FORMAT.format( metrics.toMap().get( Metrics.SAMPLE_SIZE ) ),
                    FLT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.MEAN ), unit, saneUnit, benchmark.mode() ) ),
                    INT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.MIN ), unit, saneUnit, benchmark.mode() ) ),
                    INT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.PERCENTILE_50 ), unit, saneUnit, benchmark.mode() ) ),
                    INT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.PERCENTILE_90 ), unit, saneUnit, benchmark.mode() ) ),
                    INT_FORMAT.format( Units.convertValueTo( (Double) metrics.toMap().get( Metrics.MAX ), unit, saneUnit, benchmark.mode() ) ),
                    Units.toAbbreviation( saneUnit, benchmark.mode() )};
        }
    }

    private static class RowComparator implements Comparator<String[]>
    {
        @Override
        public int compare( String[] o1, String[] o2 )
        {
            return innerCompare( o1, o2, 0 );
        }

        private int innerCompare( String[] o1, String[] o2, int offset )
        {
            if ( offset >= o1.length )
            {
                return 0;
            }
            int columnCompare = o1[offset].compareTo( o2[offset] );
            return (0 != columnCompare)
                   ? columnCompare
                   : innerCompare( o1, o2, ++offset );
        }
    }
}
