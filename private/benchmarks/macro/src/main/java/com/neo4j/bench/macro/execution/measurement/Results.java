/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Metrics.MetricsUnit;
import com.neo4j.bench.model.util.UnitConverter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.neo4j.bench.model.util.UnitConverter.toAbbreviation;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Results
{
    private static String phaseFilename( RunPhase phase )
    {
        switch ( phase )
        {
        case WARMUP:
            return "warmup-results.csv";
        case MEASUREMENT:
            return "measurement-results.csv";
        default:
            throw new IllegalStateException( format( "Unrecognized phase: %s", phase ) );
        }
    }

    public abstract static class MeasurementUnit
    {
        public static MeasurementUnit ofDuration( TimeUnit timeUnit )
        {
            return new DurationUnit( timeUnit );
        }

        public static MeasurementUnit ofCardinality()
        {
            return new CardinalityUnit();
        }

        private static MeasurementUnit parse( String value )
        {
            List<Exception> parsingErrors = new ArrayList<>();
            List<Function<String,MeasurementUnit>> parsers = Lists.newArrayList( DurationUnit::parseHeader, CardinalityUnit::parseHeader );
            for ( Function<String,MeasurementUnit> parser : parsers )
            {
                try
                {
                    return parser.apply( value );
                }
                catch ( Exception e )
                {
                    parsingErrors.add( e );
                }
            }

            IllegalStateException e = new IllegalStateException( format( "Unable to parse header:'%s'", value ) );
            parsingErrors.forEach( e::addSuppressed );
            throw e;
        }

        protected abstract String header();

        protected abstract MetricsUnit toMetricsUnit();
    }

    private static class CardinalityUnit extends MeasurementUnit
    {
        private static final String VALUE = "cardinality";

        private static MeasurementUnit parseHeader( String value )
        {
            if ( !VALUE.equals( value ) )
            {
                throw new IllegalArgumentException( format( "Invalid header column '%s'. Expected '%s'", value, VALUE ) );
            }
            return new CardinalityUnit();
        }

        @Override
        protected String header()
        {
            return VALUE;
        }

        @Override
        protected MetricsUnit toMetricsUnit()
        {
            return MetricsUnit.accuracy();
        }
    }

    private static class DurationUnit extends MeasurementUnit
    {
        private static boolean isDurationUnit( MeasurementUnit measurementUnit )
        {
            return DurationUnit.class.isAssignableFrom( measurementUnit.getClass() );
        }

        private static MeasurementUnit parseHeader( String value )
        {
            if ( !value.startsWith( PREFIX ) )
            {
                throw new IllegalStateException( format( "Invalid header column '%s'. Expected '%s' prefix", value, PREFIX ) );
            }
            int startOffset = PREFIX.length();
            String unitString = value.substring( startOffset );
            TimeUnit unit = UnitConverter.toTimeUnit( unitString );
            return new DurationUnit( unit );
        }

        private static int indexOfValidTimeUnit( TimeUnit timeUnit )
        {
            int indexOf = VALID_TIME_UNITS.indexOf( timeUnit );
            if ( indexOf < 0 )
            {
                throw new IllegalArgumentException(
                        format( "invalid time unit %s, expected one of %s", timeUnit, VALID_TIME_UNITS ) );
            }
            return indexOf;
        }

        private static int compareTimeUnits( TimeUnit tu1, TimeUnit tu2 )
        {
            int tu1Index = DurationUnit.indexOfValidTimeUnit( tu1 );
            int tu2Index = DurationUnit.indexOfValidTimeUnit( tu2 );
            return tu1Index - tu2Index;
        }

        private static final String PREFIX = "duration_";
        private static final List<TimeUnit> VALID_TIME_UNITS = Lists.newArrayList( NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS );
        private final TimeUnit unit;

        private DurationUnit( TimeUnit unit )
        {
            this.unit = unit;
        }

        @Override
        protected String header()
        {
            return PREFIX + toAbbreviation( unit );
        }

        @Override
        protected MetricsUnit toMetricsUnit()
        {
            return MetricsUnit.latency( unit );
        }
    }

    private static final String SEPARATOR = ",";
    private static final String SCHEDULED_START = "scheduled_start";
    private static final String START = "start";
    private static final String ROWS = "rows";
    private static final String HEADER_PREFIX = SCHEDULED_START + SEPARATOR + START + SEPARATOR;
    private static final String HEADER_SUFFIX = SEPARATOR + ROWS;

    public static ResultsWriter newWriter( ForkDirectory forkDirectory, RunPhase phase, MeasurementUnit measurementUnit )
    {
        Path resultsFile = forkDirectory.pathFor( phaseFilename( phase ) );
        return newWriterForFile( resultsFile, measurementUnit );
    }

    private static ResultsWriter newWriterForFile( Path resultsFile, MeasurementUnit measurementUnit )
    {
        try
        {
            BufferedWriter writer = Files.newBufferedWriter( resultsFile );
            writer.write( makeHeader( measurementUnit ) );
            writer.newLine();
            return new ResultsWriter( resultsFile, writer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error creating writer for " + resultsFile.toAbsolutePath(), e );
        }
    }

    public static Results loadFrom( BenchmarkDirectory benchmarkDirectory )
    {
        MeasurementUnit measurementUnit = measurementUnitFor( benchmarkDirectory );
        List<Double> measurements = new ArrayList<>();
        List<Long> rows = new ArrayList<>();
        benchmarkDirectory.measurementForks()
                          .forEach( f -> visitResults( f, RunPhase.MEASUREMENT, aggregateMeasurements( measurements, rows ) ) );
        return new Results(
                AggregateMeasurement.calculateFrom( measurements.stream().mapToDouble( Double::doubleValue ).toArray() ),
                AggregateMeasurement.calculateFrom( rows.stream().mapToDouble( Long::doubleValue ).toArray() ),
                measurementUnit );
    }

    public static Results loadFrom( ForkDirectory forkDirectory, RunPhase phase )
    {
        Path resultsFile = forkDirectory.findOrFail( phaseFilename( phase ) );
        return loadFromFile( resultsFile );
    }

    static Results loadFromFile( Path resultsFile )
    {
        MeasurementUnit measurementUnit = extractUnit( resultsFile );
        List<Double> measurements = new ArrayList<>();
        List<Long> rows = new ArrayList<>();
        visitResults( resultsFile, aggregateMeasurements( measurements, rows ) );
        return new Results(
                AggregateMeasurement.calculateFrom( measurements.stream().mapToDouble( Double::doubleValue ).toArray() ),
                AggregateMeasurement.calculateFrom( rows.stream().mapToDouble( Long::doubleValue ).toArray() ),
                measurementUnit );
    }

    private static MeasurementUnit measurementUnitFor( BenchmarkDirectory benchmarkDirectory )
    {
        List<MeasurementUnit> measurementUnits = benchmarkDirectory.measurementForks().stream()
                                                                   .map( forkDirectory -> forkDirectory.findOrFail( phaseFilename( RunPhase.MEASUREMENT ) ) )
                                                                   .map( Results::extractUnit )
                                                                   .collect( toList() );
        if ( DurationUnit.isDurationUnit( measurementUnits.get( 0 ) ) )
        {
            /* -- time unit manipulation -- */
            TimeUnit smallestTimeUnit = measurementUnits.stream().map( mu -> ((DurationUnit) mu).unit ).min( DurationUnit::compareTimeUnits ).get();
            return new DurationUnit( smallestTimeUnit );
        }
        else
        {
            return measurementUnits.get( 0 );
        }
    }

    private static MeasurementUnit extractUnit( Path resultsFile )
    {
        try ( BufferedReader reader = Files.newBufferedReader( resultsFile ) )
        {
            String firstLine = reader.readLine();
            assertValidHeader( firstLine );
            return extractUnitFromHeader( firstLine );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error extracting unit from: " + resultsFile.toAbsolutePath(), e );
        }
    }

    /* -- results visitor -- */
    private static void visitResults( ForkDirectory forkDirectory, RunPhase phase, ResultsRowVisitor resultsVisitor )
    {
        Path resultsFile = forkDirectory.findOrFail( phaseFilename( phase ) );
        visitResults( resultsFile, resultsVisitor );
    }

    private static void visitResults( Path resultsFile, ResultsRowVisitor resultsVisitor )
    {
        try
        {
            try ( BufferedReader reader = Files.newBufferedReader( resultsFile ) )
            {
                String firstLine = reader.readLine();
                assertValidHeader( firstLine );
            }
            LineNumberReader reader = new LineNumberReader( Files.newBufferedReader( resultsFile ) );
            reader.lines()
                  .skip( 1 ) // skip header
                  .map( line -> line.split( SEPARATOR ) )
                  .forEach( row -> resultsVisitor.visitResultsRow( resultsFile, reader::getLineNumber, row ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error reading results from " + resultsFile.toAbsolutePath(), e );
        }
    }

    private static ResultsRowVisitor aggregateMeasurements( List<Double> measurements, List<Long> rows )
    {
        return ( resultsFile, lineNumber, row ) ->
        {
            if ( row.length != 4 )
            {
                throw new RuntimeException( format(
                        "Expected 4 columns but found %s\n" +
                        "File   : %s\n" +
                        "Line # : %s\n" +
                        "Line   : %s\n" +
                        "Row    : %s",
                        row.length,
                        resultsFile.toAbsolutePath(),
                        lineNumber.get(),
                        String.join( SEPARATOR, row ),
                        Arrays.toString( row ) ) );
            }
            double measurement = Double.parseDouble( row[2] );
            long rowCount = Long.parseLong( row[3] );
            measurements.add( measurement );
            rows.add( rowCount );
        };
    }

    /* -- results headers --*/
    private static String makeHeader( MeasurementUnit measurementUnit )
    {
        return HEADER_PREFIX + measurementUnit.header() + SEPARATOR + ROWS;
    }

    private static void assertValidHeader( String header )
    {
        if ( !header.startsWith( HEADER_PREFIX ) )
        {
            throw new IllegalStateException( format( "Header should have prefix of '%s' but was '%s'", HEADER_PREFIX, header ) );
        }
        MeasurementUnit measurementUnit = extractUnitFromHeader( header );
        String suffix = header.substring( HEADER_PREFIX.length() + measurementUnit.header().length() );
        if ( !suffix.equals( HEADER_SUFFIX ) )
        {
            throw new IllegalStateException( format( "Header should have suffix of '%s' but was '%s'", HEADER_SUFFIX, header ) );
        }
    }

    private static MeasurementUnit extractUnitFromHeader( String header )
    {
        int startOffset = HEADER_PREFIX.length();
        int endOffset = header.indexOf( HEADER_SUFFIX );
        if ( startOffset >= endOffset )
        {
            throw new IllegalStateException( format( "Improperly formed header '%s'%nShould in the form of '%s<unit>%s'",
                                                     header, HEADER_PREFIX, HEADER_SUFFIX ) );
        }

        String unitString = header.substring( startOffset, endOffset );
        return MeasurementUnit.parse( unitString );
    }

    private final AggregateMeasurement measurement;
    private final AggregateMeasurement rows;
    private final MeasurementUnit measurementUnit;

    private Results( AggregateMeasurement measurement, AggregateMeasurement rows, MeasurementUnit measurementUnit )
    {
        this.measurement = measurement;
        this.rows = rows;
        this.measurementUnit = measurementUnit;
    }

    public AggregateMeasurement rows()
    {
        return rows;
    }

    public AggregateMeasurement measurement()
    {
        return measurement;
    }

    public Metrics metrics()
    {
        return new Metrics( measurementUnit.toMetricsUnit(),
                            measurement.min(),
                            measurement.max(),
                            measurement.mean(),
                            measurement.count(),
                            measurement.percentile( 0.25D ),
                            measurement.percentile( 0.50D ),
                            measurement.percentile( 0.75D ),
                            measurement.percentile( 0.90D ),
                            measurement.percentile( 0.95D ),
                            measurement.percentile( 0.99D ),
                            measurement.percentile( 0.999D ) );
    }

    public Metrics rowMetrics()
    {
        return new Metrics( MetricsUnit.rows(),
                            rows.min(),
                            rows.max(),
                            rows.mean(),
                            rows.count(),
                            rows.percentile( 0.25D ),
                            rows.percentile( 0.50D ),
                            rows.percentile( 0.75D ),
                            rows.percentile( 0.90D ),
                            rows.percentile( 0.95D ),
                            rows.percentile( 0.99D ),
                            rows.percentile( 0.999D ) );
    }

    @Override
    public String toString()
    {
        return "\nResults:" + "\n" +
               "\tmeasurement min    : " + measurement.min() + "\n" +
               "\tmeasurement mean   : " + measurement.mean() + "\n" +
               "\tmeasurement median : " + measurement.median() + "\n" +
               "\tmeasurement max    : " + measurement.max() + "\n" +
               "\t----------------\n" +
               "\trows min           : " + rows.min() + "\n" +
               "\trows mean          : " + rows.mean() + "\n" +
               "\trows median        : " + rows.median() + "\n" +
               "\trows max           : " + rows.max() + "\n";
    }

    public static final class ResultsWriter implements AutoCloseable
    {
        private final Path resultsFile;
        private final BufferedWriter writer;

        private ResultsWriter( Path resultsFile, BufferedWriter writer )
        {
            this.resultsFile = resultsFile;
            this.writer = writer;
        }

        public void write( long scheduledStartUtc, long startUtc, double measurement, long rows )
        {
            try
            {
                writer.write( scheduledStartUtc + SEPARATOR + startUtc + SEPARATOR + measurement + SEPARATOR + rows );
                writer.newLine();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Error writing to " + resultsFile.toAbsolutePath(), e );
            }
        }

        @Override
        public void close() throws Exception
        {
            writer.close();
        }
    }

    @FunctionalInterface
    private interface ResultsRowVisitor
    {
        void visitResultsRow( Path resultsFile, Supplier<Integer> lineNumber, String[] row );
    }

    /**
     * Converts the result measurements to the specified time unit.
     *
     * @param toTimeUnit the time unit to convert to
     * @return a new instance of {@link Results}, containing the converted measurements
     * @throws IllegalArgumentException if the current unit is not a time unit, e.g., if it is 'rows'
     */
    public Results convertTimeUnit( TimeUnit toTimeUnit )
    {
        if ( DurationUnit.isDurationUnit( measurementUnit ) )
        {
            TimeUnit unit = ((DurationUnit) measurementUnit).unit;
            return new Results( measurement.convert( m -> new Long( toTimeUnit.convert( Math.round( m ), unit ) ).doubleValue() ),
                                rows,
                                new DurationUnit( toTimeUnit ) );
        }
        else
        {
            throw new IllegalArgumentException( format( "Does not make sense to convert from unit '%s' to a time unit",
                                                        measurementUnit.getClass().getSimpleName() ) );
        }
    }
}
