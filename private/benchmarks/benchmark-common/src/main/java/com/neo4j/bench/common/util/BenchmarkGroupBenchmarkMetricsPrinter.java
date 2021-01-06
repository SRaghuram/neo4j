/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.util.UnitConverter;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.neo4j.bench.model.model.Metrics.MAX;
import static com.neo4j.bench.model.model.Metrics.MEAN;
import static com.neo4j.bench.model.model.Metrics.MIN;
import static com.neo4j.bench.model.model.Metrics.MetricsUnit;
import static com.neo4j.bench.model.model.Metrics.PERCENTILE_50;
import static com.neo4j.bench.model.model.Metrics.PERCENTILE_90;
import static com.neo4j.bench.model.model.Metrics.SAMPLE_SIZE;
import static com.neo4j.bench.model.model.Metrics.UNIT;
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

        List<Row> rows = new ArrayList<>();
        for ( BenchmarkGroupBenchmark benchmark : results.benchmarkGroupBenchmarks() )
        {
            BenchmarkGroupBenchmarkMetrics.AnnotatedMetrics annotatedMetrics = results.getMetricsFor( benchmark );
            String[] data = rowWriter.registerDataRow( benchmark.benchmarkGroup(), benchmark.benchmark(), annotatedMetrics.metrics() );
            String[] auxiliaryData = (null == annotatedMetrics.maybeAuxiliaryMetrics())
                                     ? null
                                     : rowWriter.registerAuxiliaryDataRow( benchmark.benchmarkGroup(),
                                                                           benchmark.benchmark(),
                                                                           annotatedMetrics.maybeAuxiliaryMetrics() );
            rows.add( new Row( data, null /* error */, auxiliaryData ) );
        }
        for ( TestRunError error : errors )
        {
            String[] errorData = rowWriter.registerErrorRow( error.groupName(), error.benchmarkName() );
            rows.add( new Row( null /* data */, errorData, null  /* auxiliary data */ ) );
        }
        rows.sort( new RowComparator() );

        StringBuilder sb = new StringBuilder();
        sb.append( rowWriter.separator() ).append( "\n" );
        sb.append( rowWriter.prettyHeader() ).append( "\n" );
        sb.append( rowWriter.separator() ).append( "\n" );

        for ( Row row : rows )
        {
            for ( String[] dataRow : row.dataRows() )
            {
                sb.append( rowWriter.prettyRow( dataRow ) ).append( "\n" );
            }
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

    private interface MetricsValueFormatter
    {
        String formatValue( Double value, DecimalFormat decimalFormat );

        String unitString();
    }

    private static class BasicFormatter implements MetricsValueFormatter
    {
        private final MetricsUnit unit;

        private BasicFormatter( MetricsUnit unit )
        {
            this.unit = unit;
        }

        @Override
        public String formatValue( Double value, DecimalFormat decimalFormat )
        {
            return decimalFormat.format( value );
        }

        @Override
        public String unitString()
        {
            return unit.value();
        }
    }

    private static class TimeUnitFormatter implements MetricsValueFormatter
    {
        private final TimeUnit unit;
        private final TimeUnit saneUnit;
        private final Benchmark.Mode mode;

        private TimeUnitFormatter( TimeUnit unit, TimeUnit saneUnit, Benchmark.Mode mode )
        {
            this.unit = unit;
            this.saneUnit = saneUnit;
            this.mode = mode;
        }

        @Override
        public String formatValue( Double value, DecimalFormat decimalFormat )
        {
            return decimalFormat.format( Units.convertValueTo( value, unit, saneUnit, mode ) );
        }

        @Override
        public String unitString()
        {
            return Units.toAbbreviation( saneUnit, mode );
        }
    }

    private static class Row
    {
        private final String[] data;
        private final String[] error;
        private final String[] auxiliaryData;

        private Row( String[] data,
                     String[] error,
                     String[] auxiliaryData )
        {
            assertDataXorError( data, error );
            assertNoAuxiliaryDataIfError( error, auxiliaryData );
            assertDataAndAuxiliaryDataSameLength( data, auxiliaryData );
            this.data = data;
            this.error = error;
            this.auxiliaryData = auxiliaryData;
        }

        private void assertDataAndAuxiliaryDataSameLength( String[] data, String[] auxiliaryData )
        {
            if ( null != data && null != auxiliaryData && data.length != auxiliaryData.length )
            {
                throw new IllegalStateException( format( "Data and auxiliary data must have same length\n" +
                                                         "Data:           %s\n" +
                                                         "Auxiliary Data: %s",
                                                         Arrays.toString( data ),
                                                         Arrays.toString( auxiliaryData ) ) );
            }
        }

        private void assertNoAuxiliaryDataIfError( String[] error, String[] auxiliaryData )
        {
            if ( null != error && null != auxiliaryData )
            {
                throw new IllegalStateException( format( "An error row must not have auxiliary data\n" +
                                                         "Error:          %s\n" +
                                                         "Auxiliary Data: %s",
                                                         Arrays.toString( error ),
                                                         Arrays.toString( auxiliaryData ) ) );
            }
        }

        private void assertDataXorError( String[] data, String[] error )
        {
            if ( (null == data) == (null == error) )
            {
                throw new IllegalStateException( format( "A row must have either data or error, not both or neither\n" +
                                                         "Data:  %s\n" +
                                                         "Error: %s",
                                                         null == data ? null : Arrays.toString( data ),
                                                         null == error ? null : Arrays.toString( error ) ) );
            }
        }

        private String[][] dataRows()
        {
            if ( data != null )
            {
                return (null == auxiliaryData)
                       ? new String[][]{data}
                       : new String[][]{data, auxiliaryData};
            }
            else
            {
                return new String[][]{error};
            }
        }

        private int length()
        {
            return null != data ? data.length : error.length;
        }

        private String elementAt( int i )
        {
            return null != data ? data[i] : error[i];
        }
    }

    private abstract static class RowWriter
    {
        private final String[] headers = headers();
        private final int[] columnWidths = Arrays.stream( headers ).mapToInt( String::length ).toArray();
        private final int columnPadding = 2;
        private String format;
        private String separator;

        abstract String[] headers();

        /**
         * Will not be called until all rows have been converted to arrays, i.e., after format has been set. Will be called once per data row.
         *
         * @return data row
         */
        abstract String[] createDataRow( BenchmarkGroup group,
                                         Benchmark benchmark,
                                         Metrics metrics,
                                         MetricsValueFormatter metricsFormatter );

        abstract String[] createAuxiliaryDataRow( BenchmarkGroup group,
                                                  Benchmark benchmark,
                                                  Metrics auxiliaryMetrics );

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
            MetricsValueFormatter metricsFormatter = getMetricsFormatterFor( benchmark, metrics );
            String[] row = createDataRow( group, benchmark, metrics, metricsFormatter );
            return updateWidths( row );
        }

        private MetricsValueFormatter getMetricsFormatterFor( Benchmark benchmark, Metrics metrics )
        {
            MetricsUnit metricsUnit = MetricsUnit.parse( (String) metrics.toMap().get( UNIT ) );
            if ( MetricsUnit.isLatency( metricsUnit ) )
            {
                TimeUnit unit = UnitConverter.toTimeUnit( metricsUnit.value() );

                // compute unit at which mean is in range [1,1000]
                double mean = (double) metrics.toMap().get( MEAN );
                TimeUnit saneUnit = Units.findSaneUnit( mean, unit, benchmark.mode(), 1, 1000 );
                return new TimeUnitFormatter( unit, saneUnit, benchmark.mode() );
            }
            else
            {
                return new BasicFormatter( metricsUnit );
            }
        }

        String[] registerAuxiliaryDataRow( BenchmarkGroup group, Benchmark benchmark, Metrics auxiliaryMetrics )
        {
            String[] row = createAuxiliaryDataRow( group, benchmark, auxiliaryMetrics );
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
        public String[] createDataRow( BenchmarkGroup group,
                                       Benchmark benchmark,
                                       Metrics metrics,
                                       MetricsValueFormatter metricsFormatter )
        {
            Map<String,Object> metricsMap = metrics.toMap();
            return new String[]{
                    group.name(),
                    benchmark.name(),
                    INT_FORMAT.format( metricsMap.get( SAMPLE_SIZE ) ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( MEAN ), FLT_FORMAT ),
                    metricsFormatter.unitString()};
        }

        @Override
        String[] createAuxiliaryDataRow( BenchmarkGroup group,
                                         Benchmark benchmark,
                                         Metrics auxiliaryMetrics )
        {
            Map<String,Object> auxiliaryMetricsMap = auxiliaryMetrics.toMap();
            return new String[]{
                    "" /* benchmark group column, do not print it again for auxiliary metrics rows */,
                    "" /* benchmark column, do not print it again for auxiliary metrics rows */,
                    "" /* count column, do not print it again for auxiliary metrics rows */,
                    FLT_FORMAT.format( auxiliaryMetricsMap.get( MEAN ) ),
                    (String) auxiliaryMetricsMap.get( UNIT )};
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
        String[] createDataRow( BenchmarkGroup group,
                                Benchmark benchmark,
                                Metrics metrics,
                                MetricsValueFormatter metricsFormatter )
        {
            Map<String,Object> metricsMap = metrics.toMap();
            return new String[]{
                    group.name(),
                    benchmark.name(),
                    INT_FORMAT.format( metricsMap.get( SAMPLE_SIZE ) ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( MEAN ), FLT_FORMAT ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( MIN ), INT_FORMAT ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( PERCENTILE_50 ), INT_FORMAT ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( PERCENTILE_90 ), INT_FORMAT ),
                    metricsFormatter.formatValue( (Double) metricsMap.get( MAX ), INT_FORMAT ),
                    metricsFormatter.unitString()};
        }

        @Override
        String[] createAuxiliaryDataRow( BenchmarkGroup group,
                                         Benchmark benchmark,
                                         Metrics auxiliaryMetrics )
        {
            Map<String,Object> auxiliaryMetricsMap = auxiliaryMetrics.toMap();
            return new String[]{
                    "" /* benchmark group column, do not print it again for auxiliary metrics rows */,
                    "" /* benchmark column, do not print it again for auxiliary metrics rows */,
                    "" /* count column, do not print it again for auxiliary metrics rows */,
                    FLT_FORMAT.format( auxiliaryMetricsMap.get( MEAN ) ),
                    INT_FORMAT.format( auxiliaryMetricsMap.get( MIN ) ),
                    INT_FORMAT.format( auxiliaryMetricsMap.get( PERCENTILE_50 ) ),
                    INT_FORMAT.format( auxiliaryMetricsMap.get( PERCENTILE_90 ) ),
                    INT_FORMAT.format( auxiliaryMetricsMap.get( MAX ) ),
                    (String) auxiliaryMetricsMap.get( UNIT )};
        }
    }

    private static class RowComparator implements Comparator<Row>
    {
        @Override
        public int compare( Row o1, Row o2 )
        {
            return innerCompare( o1, o2, 0 );
        }

        private int innerCompare( Row o1, Row o2, int offset )
        {
            if ( offset >= o1.length() )
            {
                return 0;
            }
            int columnCompare = o1.elementAt( offset ).compareTo( o2.elementAt( offset ) );
            return (0 != columnCompare)
                   ? columnCompare
                   : innerCompare( o1, o2, ++offset );
        }
    }
}
