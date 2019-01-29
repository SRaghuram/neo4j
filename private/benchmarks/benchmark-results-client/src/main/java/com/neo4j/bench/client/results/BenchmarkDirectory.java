package com.neo4j.bench.client.results;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.neo4j.bench.client.util.BenchmarkUtil.assertDirectoryExists;

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

            if ( Files.exists( dir ) )
            {
                Benchmark foundBenchmark = loadBenchmarkOrFail( dir );
                if ( !foundBenchmark.equals( benchmark ) )
                {
                    throw new RuntimeException( format( "Directory contained unexpected benchmark file\n" +
                                                        "Directory          : %s\n" +
                                                        "Expected Benchmark : %s\n" +
                                                        "Found Benchmark    : %s",
                                                        dir.toAbsolutePath(), benchmark.name(), foundBenchmark.name() ) );
                }
            }
            else
            {
                // No directory found, create new
                Files.createDirectory( dir );
                JsonUtil.serializeJson( dir.resolve( BENCHMARK_JSON ), benchmark );
            }

            return new BenchmarkDirectory( dir, benchmark );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error creating benchmark directory for '%s' in: %s", benchmark.name(), parentDir.toAbsolutePath() ), e );
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

    public ForkDirectory create( String forkName, List<ProfilerType> profilers )
    {
        return ForkDirectory.createAt( dir, forkName, profilers );
    }

    public List<ForkDirectory> measurementForks()
    {
        return forks().stream().filter( fork -> fork.profilers().isEmpty() ).collect( toList() );
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

    void copyProfilerRecordings( BenchmarkGroup benchmarkGroup, Path targetDir, Set<RecordingType> excluding )
    {
        forks().forEach( fork -> fork.copyProfilerRecordings( benchmarkGroup, benchmark, targetDir, excluding ) );
    }
}
