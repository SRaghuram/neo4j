/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.measurement.Results.MeasurementUnit;
import com.neo4j.bench.macro.execution.measurement.Results.ResultsWriter;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Metrics.MetricsUnit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
public class ResultsTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void shouldFailToLoadWhenNoFilePresent() throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT ) );
    }

    @Test
    public void shouldFailToLoadWhenResultsFileIsEmpty() throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        ResultsWriter resultsWriter = Results.newWriter( forkDirectory, RunPhase.MEASUREMENT, MeasurementUnit.ofDuration( MILLISECONDS ) );
        resultsWriter.close();
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT ) );
    }

    @Test
    public void shouldLoadResultsWhenMeasurementsFileIsProperlyFormatted() throws Exception
    {
        List<MeasurementUnit> measurementUnits = Lists.newArrayList( MeasurementUnit.ofDuration( MILLISECONDS ),
                                                                     MeasurementUnit.ofDuration( SECONDS ),
                                                                     MeasurementUnit.ofCardinality() );
        for ( MeasurementUnit measurementUnit : measurementUnits )
        {
            Path resultFile = temporaryFolder.file( "result" );
            Files.write( resultFile, Arrays.asList(
                    format( "scheduled_start,start,%s,rows", measurementUnit.header() ),
                    "1,2,3,4"
            ) );
            Results results = Results.loadFromFile( resultFile );
            assertThat( results.measurement().count(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldFailToLoadWhenInvalidHeader() throws Exception
    {
        checkInvalidHeader( "INVALID,start,duration_ms,rows" );
        checkInvalidHeader( "scheduled_start,INVALID,duration_ms,rows" );
        checkInvalidHeader( "scheduled_start,start,INVALID,rows" );
        checkInvalidHeader( "scheduled_start,start,duration_ms,INVALID" );
        checkInvalidHeader( "scheduled_start,start,rows" );
    }

    private void checkInvalidHeader( String header ) throws IOException
    {
        Path resultFile = temporaryFolder.file( "results" );
        Files.write( resultFile, Arrays.asList(
                header,
                "1,2,3,4"
        ) );
        BenchmarkUtil.assertException( IllegalStateException.class,
                                       () -> Results.loadFromFile( resultFile ) );
    }

    @Test
    public void shouldFailToLoadWhenTooFewColumns() throws Exception
    {
        Path resultFile = temporaryFolder.file( "results" );
        Files.write( resultFile, Arrays.asList(
                "scheduled_start,start,duration_ms,rows",
                "1,2,3"
        ) );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFromFile( resultFile ) );
    }

    @Test
    public void shouldFailToLoadWhenTooManyColumns() throws Exception
    {
        Path resultFile = temporaryFolder.file( "results" );
        Files.write( resultFile, Arrays.asList(
                "scheduled_start,start,duration_ms,rows",
                "1,2,3,4,5"
        ) );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFromFile( resultFile ) );
    }

    @Test
    public void shouldLoadAllHeaderVariations() throws Exception
    {
        assertRequestedHeaderIsRespected( MeasurementUnit.ofDuration( MILLISECONDS ), MetricsUnit.latency( MILLISECONDS ) );
        assertRequestedHeaderIsRespected( MeasurementUnit.ofDuration( SECONDS ), MetricsUnit.latency( SECONDS ) );
        assertRequestedHeaderIsRespected( MeasurementUnit.ofCardinality(), MetricsUnit.accuracy() );
    }

    private void assertRequestedHeaderIsRespected( MeasurementUnit requestedHeader, MetricsUnit readHeader ) throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        try ( ResultsWriter resultsWriter = Results.newWriter( forkDirectory, RunPhase.MEASUREMENT, requestedHeader ) )
        {
            resultsWriter.write( 1, 2, 3, 5 );
        }
        Results results = Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
        assertThat( results.metrics().toMap().get( Metrics.UNIT ), equalTo( readHeader.value() ) );
        assertThat( results.measurement().count(), equalTo( 1L ) );
    }

    @Test
    public void shouldWriteAndRead() throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        try ( ResultsWriter resultsWriter = Results.newWriter( forkDirectory, RunPhase.MEASUREMENT, MeasurementUnit.ofDuration( MILLISECONDS ) ) )
        {
            resultsWriter.write( 1, 2, 3, 5 );
            resultsWriter.write( 2, 2, 5, 4 );
            resultsWriter.write( 2, 2, 7, 3 );
        }

        Results results = Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
        assertThat( results.measurement().mean(), equalTo( 5D ) );
        assertThat( results.rows().mean(), equalTo( 4D ) );
    }

    @Test
    public void shouldConvertUnit() throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        try ( ResultsWriter resultsWriter = Results.newWriter( forkDirectory, RunPhase.MEASUREMENT, MeasurementUnit.ofDuration( MILLISECONDS ) ) )
        {
            resultsWriter.write( 1, 2, 30_000, 5 );
            resultsWriter.write( 2, 2, 50_000, 4 );
            resultsWriter.write( 2, 2, 70_000, 3 );
        }

        Results convertedResults = Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT ).convertTimeUnit( SECONDS );
        assertThat( convertedResults.measurement().mean(), equalTo( 50D ) );
        assertThat( convertedResults.rows().mean(), equalTo( 4D ) );
    }

    @Test
    public void shouldFailToConvertNonTimeUnit() throws Exception
    {
        ForkDirectory forkDirectory = createForkDirectory();
        try ( ResultsWriter resultsWriter = Results.newWriter( forkDirectory, RunPhase.MEASUREMENT, MeasurementUnit.ofCardinality() ) )
        {
            resultsWriter.write( 1, 2, 30_000, 5 );
            resultsWriter.write( 2, 2, 50_000, 4 );
            resultsWriter.write( 2, 2, 70_000, 3 );
        }

        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT ).convertTimeUnit( SECONDS ) );
    }

    private static final BenchmarkGroup GROUP = new BenchmarkGroup( "group 1" );
    private static final Benchmark BENCH = Benchmark.benchmarkFor( "a benchmark", "bench 1", Benchmark.Mode.LATENCY, new HashMap<>() );
    private static final String FORK = "some fork";

    private ForkDirectory createForkDirectory()
    {
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( temporaryFolder.directory( "fork" ) , GROUP );
        BenchmarkDirectory benchDir = groupDir.findOrCreate( BENCH );
        return benchDir.create( FORK );
    }
}
