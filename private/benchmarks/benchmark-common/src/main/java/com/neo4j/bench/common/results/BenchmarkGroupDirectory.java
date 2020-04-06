/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.common.profiling.NoOpProfiler;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public class BenchmarkGroupDirectory
{
    private static final String BENCHMARK_GROUP_JSON = "benchmark_group.json";

    public static List<BenchmarkGroupDirectory> searchAllIn( Path dir )
    {
        return findInnerDirs( dir ).stream()
                                   .filter( BenchmarkGroupDirectory::isBenchmarkGroupDir )
                                   .map( BenchmarkGroupDirectory::new )
                                   .collect( toList() );
    }

    private static boolean isBenchmarkGroupDir( Path dir )
    {
        return Files.exists( dir.resolve( BENCHMARK_GROUP_JSON ) );
    }

    private static BenchmarkGroup loadBenchmarkGroup( Path dir )
    {
        Path benchmarkGroupJson = dir.resolve( BENCHMARK_GROUP_JSON );
        BenchmarkUtil.assertFileExists( benchmarkGroupJson );
        return JsonUtil.deserializeJson( benchmarkGroupJson, BenchmarkGroup.class );
    }

    public static BenchmarkGroupDirectory findOrCreateAt( Path parentDir, BenchmarkGroup benchmarkGroup )
    {
        try
        {
            Path dir = parentDir.resolve( nameFor( benchmarkGroup ) );
            Optional<BenchmarkGroupDirectory> maybeBenchmarkGroupDirectory = tryOpenAt( dir, benchmarkGroup );
            if ( !maybeBenchmarkGroupDirectory.isPresent() )
            {
                // No directory found, create new
                Files.createDirectory( dir );
                JsonUtil.serializeJson( dir.resolve( BENCHMARK_GROUP_JSON ), benchmarkGroup );
                return new BenchmarkGroupDirectory( dir );
            }
            else
            {
                return maybeBenchmarkGroupDirectory.get();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error creating benchmark group directory for '%s' in: %s",
                                                benchmarkGroup.name(), parentDir.toAbsolutePath() ), e );
        }
    }

    public static BenchmarkGroupDirectory findOrFailAt( Path parentDir, BenchmarkGroup benchmarkGroup )
    {
        Path dir = parentDir.resolve( nameFor( benchmarkGroup ) );
        Optional<BenchmarkGroupDirectory> maybeBenchmarkGroupDirectory = tryOpenAt( dir, benchmarkGroup );
        if ( !maybeBenchmarkGroupDirectory.isPresent() )
        {
            throw new RuntimeException( format( "Could not find benchmark group directory for '%s' in: %s",
                                                benchmarkGroup.name(), parentDir.toAbsolutePath() ) );
        }
        return new BenchmarkGroupDirectory( dir );
    }

    private static Optional<BenchmarkGroupDirectory> tryOpenAt( Path benchmarkGroupDir, BenchmarkGroup benchmarkGroup )
    {
        if ( Files.exists( benchmarkGroupDir ) )
        {
            BenchmarkGroup foundBenchmarkGroup = loadBenchmarkGroup( benchmarkGroupDir );
            if ( !foundBenchmarkGroup.equals( benchmarkGroup ) )
            {
                throw new RuntimeException( format( "Directory contained unexpected benchmark group file\n" +
                                                    "Directory                : %s\n" +
                                                    "Expected Benchmark Group : %s\n" +
                                                    "Found Benchmark Group    : %s",
                                                    benchmarkGroupDir.toAbsolutePath(), benchmarkGroup.name(), foundBenchmarkGroup.name() ) );
            }
            return Optional.of( new BenchmarkGroupDirectory( benchmarkGroupDir ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    public static BenchmarkGroupDirectory createAt( Path parentDir, BenchmarkGroup benchmarkGroup )
    {
        try
        {
            Path dir = parentDir.resolve( nameFor( benchmarkGroup ) );
            if ( Files.exists( dir ) )
            {
                BenchmarkUtil.deleteDir( dir );
            }
            Files.createDirectory( dir );
            JsonUtil.serializeJson( dir.resolve( BENCHMARK_GROUP_JSON ), benchmarkGroup );
            return new BenchmarkGroupDirectory( dir );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error creating result directory for workload '%s' in %s",
                                                    benchmarkGroup.name(), parentDir.toAbsolutePath() ), e );
        }
    }

    private static String nameFor( BenchmarkGroup benchmarkGroup )
    {
        return BenchmarkUtil.sanitize( benchmarkGroup.name() );
    }

    private final Path dir;

    private BenchmarkGroupDirectory( Path dir )
    {
        this.dir = dir;
    }

    public List<Benchmark> benchmarks()
    {
        return benchmarksDirectories().stream()
                                      .map( BenchmarkDirectory::benchmark )
                                      .collect( toList() );
    }

    public List<BenchmarkDirectory> benchmarksDirectories()
    {
        return findInnerDirs( dir ).stream()
                                   .filter( BenchmarkDirectory::isBenchmarkDir )
                                   .map( BenchmarkDirectory::openAt )
                                   .collect( toList() );
    }

    public BenchmarkGroup benchmarkGroup()
    {
        return BenchmarkGroupDirectory.loadBenchmarkGroup( dir );
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    public BenchmarkDirectory findOrCreate( Benchmark benchmark )
    {
        return BenchmarkDirectory.findOrCreateAt( dir, benchmark );
    }

    public BenchmarkDirectory findOrFail( Benchmark benchmark )
    {
        return BenchmarkDirectory.findOrFailAt( dir, benchmark );
    }

    public void copyProfilerRecordings( Path targetDir )
    {
        copyProfilerRecordings( targetDir, emptySet() );
    }

    public void copyProfilerRecordings( Path targetDir, Set<RecordingType> excluding )
    {
        Set<RecordingType> excludingPlusNone = new HashSet<>( excluding );
        /**
         * No need to copy/keep 'none' recording type
         * <p>
         * See {@link NoOpProfiler} for explanation about why this profiler is required.
         */
        excludingPlusNone.add( RecordingType.NONE );
        BenchmarkGroup benchmarkGroup = benchmarkGroup();
        benchmarksDirectories().forEach( benchmarkDirectory -> benchmarkDirectory.copyProfilerRecordings( benchmarkGroup, targetDir, excludingPlusNone ) );
    }

    private static List<Path> findInnerDirs( Path dir )
    {
        try ( Stream<Path> entries = Files.list( dir ) )
        {
            return entries
                    .filter( Files::isDirectory )
                    .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error retrieving directories", e );
        }
    }
}
