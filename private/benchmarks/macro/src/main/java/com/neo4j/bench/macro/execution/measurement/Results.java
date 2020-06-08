/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.model.model.AuxiliaryMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.common.results.BenchmarkDirectory;
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
import java.util.function.Supplier;

import static com.neo4j.bench.model.util.UnitConverter.toAbbreviation;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class Results
{
    public enum Phase
    {

        WARMUP( "warmup-results.csv" ),
        MEASUREMENT( "measurement-results.csv" );

        private final String filename;

        Phase( String filename )
        {
            this.filename = filename;
        }
    }

    private static final String SEPARATOR = ",";
    private static final String SCHEDULED_START = "scheduled_start";
    private static final String START = "start";
    private static final String DURATION = "duration";
    private static final String ROWS = "rows";
    private static final List<TimeUnit> VALID_UNITS =
            Lists.newArrayList( NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS );

    public static ResultsWriter newWriter( ForkDirectory forkDirectory, Phase phase, TimeUnit unit )
    {
        Path resultsFile = forkDirectory.pathFor( phase.filename );
        return newWriterForFile( resultsFile, unit );
    }

    private static ResultsWriter newWriterForFile( Path resultsFile, TimeUnit unit )
    {
        try
        {
            BufferedWriter writer = Files.newBufferedWriter( resultsFile );
            writer.write( makeHeader( unit ) );
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
        TimeUnit smallestTimeUnit = smallestTimeUnit( benchmarkDirectory );
        List<Long> durations = new ArrayList<>();
        List<Long> rows = new ArrayList<>();
        benchmarkDirectory.measurementForks()
                          .stream()
                          .forEach( f -> visitResults( f, Results.Phase.MEASUREMENT, aggregateMeasurements( durations, rows ) ) );
        return new Results(
                AggregateMeasurement.calculateFrom( durations.stream().mapToLong( Long::longValue ).toArray() ),
                AggregateMeasurement.calculateFrom( rows.stream().mapToLong( Long::longValue ).toArray() ),
                smallestTimeUnit );
    }

    public static Results loadFrom( ForkDirectory forkDirectory, Phase phase )
    {
        Path resultsFile = forkDirectory.findOrFail( phase.filename );
        return loadFromFile( resultsFile );
    }

    static Results loadFromFile( Path resultsFile )
    {
        TimeUnit timeUnit = extractUnit( resultsFile );
        List<Long> durations = new ArrayList<>();
        List<Long> rows = new ArrayList<>();
        visitResults( resultsFile, timeUnit, aggregateMeasurements( durations, rows ) );
        return new Results(
                AggregateMeasurement.calculateFrom( durations.stream().mapToLong( Long::longValue ).toArray() ),
                AggregateMeasurement.calculateFrom( rows.stream().mapToLong( Long::longValue ).toArray() ), timeUnit );
    }

    public static Results empty()
    {
        return new Results( AggregateMeasurement.createEmpty(), AggregateMeasurement.createEmpty(), MICROSECONDS );
    }

    /* -- time unit manipulation -- */
    private static TimeUnit smallestTimeUnit( BenchmarkDirectory benchmarkDirectory )
    {
        return benchmarkDirectory.measurementForks().stream()
                                 .map( forkDirectory -> forkDirectory.findOrFail( Phase.MEASUREMENT.filename ) )
                                 .map( Results::extractUnit )
                                 .min( Results::compareTimeUnits )
                                 .get();
    }

    private static int compareTimeUnits( TimeUnit tu1, TimeUnit tu2 )
    {
        int tu1Index = indexOfValidTimeUnit( tu1 );
        int tu2Index = indexOfValidTimeUnit( tu2 );
        return tu1Index - tu2Index;
    }

    private static int indexOfValidTimeUnit( TimeUnit timeUnit )
    {
        int indexOf = VALID_UNITS.indexOf( timeUnit );
        if ( indexOf < 0 )
        {
            throw new IllegalArgumentException(
                    format( "invalid time unit %s, expected one of %s", timeUnit, VALID_UNITS ) );
        }
        return indexOf;
    }

    private static TimeUnit extractUnit( Path resultsFile )
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
    private static void visitResults( ForkDirectory forkDirectory, Phase phase, ResultsRowVisitor resultsVisitor )
    {
        Path resultsFile = forkDirectory.findOrFail( phase.filename );
        TimeUnit fromTimeUnit = extractUnit( resultsFile );
        visitResults( resultsFile, fromTimeUnit, resultsVisitor );
    }

    private static void visitResults( Path resultsFile, TimeUnit timeUnit, ResultsRowVisitor resultsVisitor )
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
                  .forEach( row ->
                            {
                                resultsVisitor.visitResultsRow( resultsFile, reader::getLineNumber, row );
                            } );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error reading results from " + resultsFile.toAbsolutePath(), e );
        }
    }

    private static ResultsRowVisitor aggregateMeasurements( List<Long> durations, List<Long> rows )
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
                        Arrays.stream( row ).collect( joining( SEPARATOR ) ),
                        Arrays.toString( row ) ) );
            }
            long duration = Long.parseLong( row[2] );
            long rowCount = Long.parseLong( row[3] );
            durations.add( duration );
            rows.add( rowCount );
        };
    }

    /* -- results headers --*/
    private static String makeHeader( TimeUnit unit )
    {
        return SCHEDULED_START + SEPARATOR + START + SEPARATOR + DURATION + "_" + toAbbreviation( unit ) + SEPARATOR
               + ROWS;
    }

    private static void assertValidHeader( String header )
    {
        if ( !isValidHeader( header ) )
        {
            throw new RuntimeException( "Invalid header: " + header );
        }
    }

    private static boolean isValidHeader( String header )
    {
        return VALID_UNITS.stream().anyMatch( unit -> header.equals( makeHeader( unit ) ) );
    }

    private static TimeUnit extractUnitFromHeader( String header )
    {
        int startOffset = (SCHEDULED_START + SEPARATOR + START + SEPARATOR + DURATION).length() + 1;
        int endOffset = header.lastIndexOf( SEPARATOR );
        String unitString = header.substring( startOffset, endOffset );
        return UnitConverter.toTimeUnit( unitString );
    }

    private final AggregateMeasurement duration;
    private final AggregateMeasurement rows;
    private final TimeUnit unit;

    private Results( AggregateMeasurement duration, AggregateMeasurement rows, TimeUnit unit )
    {
        this.duration = duration;
        this.rows = rows;
        this.unit = unit;
    }

    public TimeUnit unit()
    {
        return unit;
    }

    public AggregateMeasurement rows()
    {
        return rows;
    }

    public AggregateMeasurement duration()
    {
        return duration;
    }

    public Metrics metrics()
    {
        return new Metrics(
                unit,
                duration.min(),
                duration.max(),
                duration.mean(),
                duration.count(),
                duration.percentile( 0.25D ),
                duration.percentile( 0.50D ),
                duration.percentile( 0.75D ),
                duration.percentile( 0.90D ),
                duration.percentile( 0.95D ),
                duration.percentile( 0.99D ),
                duration.percentile( 0.999D ) );
    }

    public AuxiliaryMetrics rowMetrics()
    {
        return new AuxiliaryMetrics( "rows",
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
               "\tduration min    : " + duration.min() + "\n" +
               "\tduration mean   : " + duration.mean() + "\n" +
               "\tduration median : " + duration.median() + "\n" +
               "\tduration max    : " + duration.max() + "\n" +
               "\t----------------\n" +
               "\trows min        : " + rows.min() + "\n" +
               "\trows mean       : " + rows.mean() + "\n" +
               "\trows median     : " + rows.median() + "\n" +
               "\trows max        : " + rows.max() + "\n";
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

        public void write( long scheduledStartUtc, long startUtc, long duration, long rows )
        {
            try
            {
                writer.write( scheduledStartUtc + SEPARATOR + startUtc + SEPARATOR + duration + SEPARATOR + rows );
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

    public Results convertUnit( TimeUnit toTimeUnit )
    {
        return new Results( duration.convertUnit( unit, toTimeUnit ), rows, toTimeUnit );
    }
}
