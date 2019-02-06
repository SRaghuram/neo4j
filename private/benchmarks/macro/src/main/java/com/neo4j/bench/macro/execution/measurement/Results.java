/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.Units;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.results.BenchmarkDirectory;
import com.neo4j.bench.client.results.ForkDirectory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.client.Units.toAbbreviation;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

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
    private static final List<TimeUnit> VALID_UNITS = Lists.newArrayList( NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS );

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
        return benchmarkDirectory.measurementForks()
                                 .stream()
                                 .map( f -> loadFrom( f, Results.Phase.MEASUREMENT ) )
                                 .reduce( Results.empty(), Results::merge );
    }

    public static Results loadFrom( ForkDirectory forkDirectory, Phase phase )
    {
        Path resultsFile = forkDirectory.findOrFail( phase.filename );
        return loadFromFile( resultsFile );
    }

    public static Results empty()
    {
        return new Results( new ArrayList<>(),
                            AggregateMeasurement.createEmpty(),
                            AggregateMeasurement.createEmpty(),
                            MICROSECONDS );
    }

    static Results loadFromFile( Path resultsFile )
    {
        TimeUnit unit = extractUnit( resultsFile );
        List<Result> results = readResults( resultsFile );
        return createFromResults( results, unit );
    }

    private static Results createFromResults( List<Result> results, TimeUnit unit )
    {
        List<Long> durations = results.stream().map( Result::duration ).collect( toList() );
        List<Long> rows = results.stream().map( Result::rows ).collect( toList() );
        return new Results( results,
                            AggregateMeasurement.calculateFrom( durations ),
                            AggregateMeasurement.calculateFrom( rows ),
                            unit );
    }

    private static List<Result> convertResultsUnit( TimeUnit from, TimeUnit to, List<Result> results )
    {
        return results.stream()
                      .map( result -> new Result(
                              result.scheduledStartUtc(),
                              result.startUtc(),
                              to.convert( result.duration(), from ),
                              result.rows() ) )
                      .collect( toList() );
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

    private static List<Result> readResults( Path resultsFile )
    {
        try ( BufferedReader reader = Files.newBufferedReader( resultsFile ) )
        {
            String firstLine = reader.readLine();
            assertValidHeader( firstLine );

            List<Result> results = new ArrayList<>();
            String line;
            while ( null != (line = reader.readLine()) )
            {
                String[] row = line.split( SEPARATOR );
                if ( row.length != 4 )
                {
                    throw new RuntimeException( format( "Expected 4 columns but found %s\n" +
                                                        "File   : %s\n" +
                                                        "Line # : %s\n" +
                                                        "Line   : %s\n" +
                                                        "Row    : %s",
                                                        row.length,
                                                        resultsFile.toAbsolutePath(),
                                                        results.size() + 1,
                                                        line,
                                                        Arrays.toString( row ) ) );
                }
                long scheduledStartUtc = Long.parseLong( row[0] );
                long startUtc = Long.parseLong( row[1] );
                long stopUtc = Long.parseLong( row[2] );
                long rows = Long.parseLong( row[3] );
                results.add( new Result( scheduledStartUtc, startUtc, stopUtc, rows ) );
            }
            return results;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error reading results from " + resultsFile.toAbsolutePath(), e );
        }
    }

    private static String makeHeader( TimeUnit unit )
    {
        return SCHEDULED_START + SEPARATOR + START + SEPARATOR + DURATION + "_" + toAbbreviation( unit ) + SEPARATOR + ROWS;
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
        return Units.toTimeUnit( unitString );
    }

    private final List<Result> results;
    private final AggregateMeasurement duration;
    private final AggregateMeasurement rows;
    private final TimeUnit unit;

    private Results( List<Result> results, AggregateMeasurement duration, AggregateMeasurement rows, TimeUnit unit )
    {
        this.results = results;
        this.duration = duration;
        this.rows = rows;
        this.unit = unit;
    }

    public Results merge( Results other )
    {
        TimeUnit smallestUnit = smallestUnit( this.unit(), other.unit() );
        List<Result> mergedResults = new ArrayList<>();
        mergedResults.addAll( convertResultsUnit( this.unit, smallestUnit, this.results ) );
        mergedResults.addAll( convertResultsUnit( other.unit, smallestUnit, other.results ) );
        return createFromResults( mergedResults, smallestUnit );
    }

    public Results convertUnit( TimeUnit newUnit )
    {
        return createFromResults( convertResultsUnit( unit, newUnit, results ), newUnit );
    }

    private static TimeUnit smallestUnit( TimeUnit unit1, TimeUnit unit2 )
    {
        int smallestIndex = Math.min( VALID_UNITS.indexOf( unit1 ),
                                      VALID_UNITS.indexOf( unit2 ) );
        return VALID_UNITS.get( smallestIndex );
    }

    public TimeUnit unit()
    {
        return unit;
    }

    public List<Result> results()
    {
        return results;
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
                0, // TODO error
                0, // TODO error confidence OR remove error confidence entirely?
                duration.count(),
                duration.percentile( 0.25D ),
                duration.percentile( 0.50D ),
                duration.percentile( 0.75D ),
                duration.percentile( 0.90D ),
                duration.percentile( 0.95D ),
                duration.percentile( 0.99D ),
                duration.percentile( 0.999D ) );
    }

    @Override
    public String toString()
    {
        return "Results:" + "\n" +
               "\tmin    : " + duration.min() + "\n" +
               "\tmean   : " + duration.mean() + "\n" +
               "\tmedian : " + duration.median() + "\n" +
               "\tmax    : " + duration.max() + "\n" +
               "\trows   : " + rows.mean();
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
}
