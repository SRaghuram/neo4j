/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.macro.execution.measurement.Results.Phase;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
public class ResultsTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void shouldFailToLoadWhenNoHeader() throws Exception
    {
        Path existingForkDirPath = createForkDirPath();
        ForkDirectory forkDirectory = ForkDirectory.openAt( existingForkDirPath );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFrom( forkDirectory, Phase.MEASUREMENT ) );
    }

    @Test
    public void shouldFailToLoadWhenNoResults() throws Exception
    {
        Path existingForkDirPath = createForkDirPath();
        ForkDirectory forkDirectory = ForkDirectory.openAt( existingForkDirPath );
        Results.ResultsWriter resultsWriter = Results.newWriter( forkDirectory, Phase.MEASUREMENT, MILLISECONDS );
        resultsWriter.close();
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFrom( forkDirectory, Phase.MEASUREMENT ) );
    }

    @Test
    public void shouldBeAbleToCreateEmptyResults()
    {
        Results empty = Results.empty();
        assertThat( empty.rows().count(), equalTo( 0L ) );
        assertThat( empty.duration().count(), equalTo( 0L ) );
    }

    @Test
    public void shouldFailToLoadWhenTooFewColumns() throws Exception
    {
        File validResultFile = temporaryFolder.file( "valid-result" ).toFile();
        try ( BufferedWriter bufferedWriter = Files.newBufferedWriter( validResultFile.toPath() ) )
        {
            bufferedWriter.write( "scheduled_start,start,duration_ms,rows" );
            bufferedWriter.newLine();
            bufferedWriter.write( "1,2,3,4" );
        }
        Results.loadFromFile( validResultFile.toPath() );

        File invalidResultFile = temporaryFolder.file( "invalid-results" ).toFile();
        try ( BufferedWriter bufferedWriter = Files.newBufferedWriter( invalidResultFile.toPath() ) )
        {
            bufferedWriter.write( "scheduled_start,start,duration_invalid,rows" );
            bufferedWriter.newLine();
            bufferedWriter.write( "1,2,3" );
        }
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFromFile( invalidResultFile.toPath() ) );
    }

    @Test
    public void shouldFailToLoadWhenTooManyColumns() throws Exception
    {
        File validResultFile = Files.createTempFile( temporaryFolder.absolutePath(), "valid-results", ".file" ).toFile();
        try ( BufferedWriter bufferedWriter = Files.newBufferedWriter( validResultFile.toPath() ) )
        {
            bufferedWriter.write( "scheduled_start,start,duration_ms,rows" );
            bufferedWriter.newLine();
            bufferedWriter.write( "1,2,3,4" );
        }
        Results.loadFromFile( validResultFile.toPath() );

        File invalidResultFile = temporaryFolder.file( "invalid-results" ).toFile();
        try ( BufferedWriter bufferedWriter = Files.newBufferedWriter( invalidResultFile.toPath() ) )
        {
            bufferedWriter.write( "scheduled_start,start,duration_invalid,rows" );
            bufferedWriter.newLine();
            bufferedWriter.write( "1,2,3,4,5" );
        }
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> Results.loadFromFile( invalidResultFile.toPath() ) );
    }

    @Test
    public void shouldWriteAndRead() throws Exception
    {
        Path existingForkDirPath = createForkDirPath();
        ForkDirectory forkDirectory = ForkDirectory.openAt( existingForkDirPath );
        try ( Results.ResultsWriter resultsWriter = Results.newWriter( forkDirectory, Phase.MEASUREMENT, MILLISECONDS ) )
        {
            resultsWriter.write( 1, 2, 3, 5 );
            resultsWriter.write( 2, 2, 5, 4 );
            resultsWriter.write( 2, 2, 7, 3 );
        }

        Results results = Results.loadFrom( forkDirectory, Phase.MEASUREMENT );
        assertThat( results.duration().mean(), equalTo( 5D ) );
        assertThat( results.rows().mean(), equalTo( 4D ) );
    }

    @Test
    public void shouldCalculateAggregate()
    {
        long[] measurements = new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};
        AggregateMeasurement aggregate = AggregateMeasurement.calculateFrom( measurements );

        assertThat( aggregate.percentile( 0.0D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.1D ), equalTo( 2L ) );
        assertThat( aggregate.percentile( 0.2D ), equalTo( 3L ) );
        assertThat( aggregate.percentile( 0.3D ), equalTo( 4L ) );
        assertThat( aggregate.percentile( 0.4D ), equalTo( 5L ) );
        assertThat( aggregate.percentile( 0.5D ), equalTo( 6L ) );
        assertThat( aggregate.percentile( 0.6D ), equalTo( 7L ) );
        assertThat( aggregate.percentile( 0.7D ), equalTo( 8L ) );
        assertThat( aggregate.percentile( 0.8D ), equalTo( 9L ) );
        assertThat( aggregate.percentile( 0.9D ), equalTo( 10L ) );
        assertThat( aggregate.percentile( 1.0D ), equalTo( 10L ) );

        assertThat( aggregate.mean(), equalTo( 5.5D ) );
        assertThat( aggregate.min(), equalTo( 1L ) );
        assertThat( aggregate.median(), equalTo( 6L ) );
        assertThat( aggregate.max(), equalTo( 10L ) );
    }

    @Test
    public void shouldCalculateSingleResultAggregate()
    {
        long[] measurements = new long[]{1L};
        AggregateMeasurement aggregate = AggregateMeasurement.calculateFrom( measurements );

        assertThat( aggregate.percentile( 0.0D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.1D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.2D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.3D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.4D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.5D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.6D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.7D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.8D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 0.9D ), equalTo( 1L ) );
        assertThat( aggregate.percentile( 1.0D ), equalTo( 1L ) );

        assertThat( aggregate.mean(), equalTo( 1D ) );
        assertThat( aggregate.min(), equalTo( 1L ) );
        assertThat( aggregate.median(), equalTo( 1L ) );
        assertThat( aggregate.max(), equalTo( 1L ) );
    }

    private static final BenchmarkGroup GROUP = new BenchmarkGroup( "group 1" );
    private static final Benchmark BENCH = Benchmark.benchmarkFor( "a benchmark", "bench 1", Benchmark.Mode.LATENCY, new HashMap<>() );
    private static final String FORK = "some fork";

    private Path createForkDirPath() throws IOException
    {
        BenchmarkGroupDirectory groupDir = BenchmarkGroupDirectory.createAt( temporaryFolder.directory( "fork" ) , GROUP );
        BenchmarkDirectory benchDir = groupDir.findOrCreate( BENCH );
        ForkDirectory forkDir = benchDir.create( FORK, new ArrayList<>() );
        return Paths.get( forkDir.toAbsolutePath() );
    }
}
