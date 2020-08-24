/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryExists;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class BenchmarkDirectory
{
    private static final String BENCHMARK_JSON = "benchmark.json";

    static BenchmarkDirectory openAt( Path dir )
    {
        Benchmark benchmark = loadBenchmarkOrFail( dir );
        return new BenchmarkDirectory( dir, benchmark );
    }

    static boolean isBenchmarkDir( Path dir )
    {
        return Files.exists( dir ) && Files.exists( dir.resolve( BENCHMARK_JSON ) );
    }

    static BenchmarkDirectory findOrCreateAt( Path parentDir, Benchmark benchmark )
    {
        try
        {
            Path dir = parentDir.resolve( nameFor( benchmark ) );
            Optional<BenchmarkDirectory> maybeBenchmark = tryOpenAt( dir, benchmark );
            if ( !maybeBenchmark.isPresent() )
            {
                // No directory found, create new
                Files.createDirectory( dir );
                JsonUtil.serializeJson( dir.resolve( BENCHMARK_JSON ), benchmark );
                return new BenchmarkDirectory( dir, benchmark );
            }
            else
            {
                return maybeBenchmark.get();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error creating benchmark directory for '%s' in: %s", benchmark.name(), parentDir.toAbsolutePath() ), e );
        }
    }

    static BenchmarkDirectory findOrFailAt( Path parentDir, Benchmark benchmark )
    {
        Path dir = parentDir.resolve( nameFor( benchmark ) );
        Optional<BenchmarkDirectory> maybeBenchmarkDirectory = tryOpenAt( dir, benchmark );
        if ( !maybeBenchmarkDirectory.isPresent() )
        {
            throw new RuntimeException( format( "Could not find benchmark directory for '%s' in: %s",
                                                benchmark.name(), parentDir.toAbsolutePath() ) );
        }

        return new BenchmarkDirectory( dir, benchmark );
    }

    private static Optional<BenchmarkDirectory> tryOpenAt( Path benchmarkDir, Benchmark benchmark )
    {
        if ( Files.exists( benchmarkDir ) )
        {
            Benchmark foundBenchmark = loadBenchmarkOrFail( benchmarkDir );
            if ( !foundBenchmark.equals( benchmark ) )
            {
                throw new RuntimeException( format( "Directory contained unexpected benchmark file\n" +
                                                    "Directory          : %s\n" +
                                                    "Expected Benchmark : %s\n" +
                                                    "Found Benchmark    : %s",
                                                    benchmarkDir.toAbsolutePath(), benchmark.name(), foundBenchmark.name() ) );
            }
            return Optional.of( new BenchmarkDirectory( benchmarkDir, benchmark ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    private static Benchmark loadBenchmarkOrFail( Path dir )
    {
        assertDirectoryExists( dir );
        Path benchmarkJson = dir.resolve( BENCHMARK_JSON );
        // Found directory, make sure it is as expected
        BenchmarkUtil.assertFileExists( benchmarkJson );
        return JsonUtil.deserializeJson( benchmarkJson, Benchmark.class );
    }

    private static String nameFor( Benchmark benchmark )
    {
        return BenchmarkUtil.sanitize( benchmark.name() );
    }

    private final Path dir;
    private final Benchmark benchmark;

    private BenchmarkDirectory( Path dir, Benchmark benchmark )
    {
        this.dir = dir;
        this.benchmark = benchmark;
    }

    public Benchmark benchmark()
    {
        return benchmark;
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    public ForkDirectory findOrFail( String forkName )
    {
        return ForkDirectory.findOrFailAt( dir, forkName );
    }

    public ForkDirectory findOrCreate( String forkName )
    {
        return ForkDirectory.findOrCreateAt( dir, forkName );
    }

    public ForkDirectory create( String forkName )
    {
        return ForkDirectory.createAt( dir, forkName );
    }

    public List<ForkDirectory> measurementForks()
    {
        return forks().stream()
                      .filter( ForkDirectory::isMeasurementFork )
                      .collect( toList() );
    }

    public List<Path> plans()
    {
        return forks().stream()
                      .filter( fork -> Files.exists( fork.pathForPlan() ) )
                      .map( ForkDirectory::pathForPlan )
                      .collect( toList() );
    }

    public List<ForkDirectory> forks()
    {
        try ( Stream<Path> entries = Files.list( dir ) )
        {
            return entries
                    .filter( Files::isDirectory )
                    .map( ForkDirectory::openAt )
                    .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error retrieving fork directories", e );
        }
    }
}
