/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.profiling.FullBenchmarkName;
import com.neo4j.bench.client.profiling.RecordingType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.String.format;

public class ProfileLoader
{
    public static Map<BenchmarkGroupBenchmark,ProfilerRecordings> searchProfiles(
            Path profilesDir,
            String s3Prefix,
            Set<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks,
            boolean ignoreUnrecognizedFiles )
    {
        String s3Bucket = s3Prefix.endsWith( "/" ) ? s3Prefix : s3Prefix + "/";

        List<String> errors = new ArrayList<>();

        Map<BenchmarkGroupBenchmark,ProfilerRecordings> benchmarkProfiles = new HashMap<>();

        Consumer<Path> loadBenchmarkProfiles = path ->
        {
            boolean found = false;
            String filename = path.getFileName().toString();
            for ( BenchmarkGroupBenchmark benchmarkGroupBenchmark : benchmarkGroupBenchmarks )
            {
                for ( RecordingType recordingType : RecordingType.values() )
                {
                    // Some tools need to sanitize filenames due to requirements from, for example, JFR/Async
                    FullBenchmarkName benchmarkName = FullBenchmarkName.from( benchmarkGroupBenchmark );
                    Set<String> expectedFilenames = Sets.newHashSet(
                            benchmarkName.name() + recordingType.extension(),
                            benchmarkName.sanitizedName() + recordingType.extension() );
                    if ( expectedFilenames.contains( filename ) )
                    {
                        ProfilerRecordings profilerRecordings = benchmarkProfiles.computeIfAbsent(
                                benchmarkGroupBenchmark,
                                bgb -> new ProfilerRecordings() );
                        benchmarkProfiles.put(
                                benchmarkGroupBenchmark,
                                profilerRecordings.with( recordingType, s3Bucket + filename ) );
                        found = true;
                    }
                }
            }

            if ( !found )
            {
                errors.add( format( "WARNING: Unrecognized file with recognized suffix\n" +
                                    "\tFilename: %s", filename ) );
            }
        };

        try ( Stream<Path> paths = Files.list( profilesDir )
                                        .filter( Files::isRegularFile )
                                        .filter( ProfileLoader::hasRecognizedSuffix ) )
        {
            paths.forEach( loadBenchmarkProfiles );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        if ( errors.isEmpty() )
        {
            return benchmarkProfiles;
        }
        else if ( ignoreUnrecognizedFiles )
        {
            errors.forEach( System.out::println );
            return benchmarkProfiles;
        }
        else
        {
            String errorMessage = String.join( "\n", errors );
            throw new RuntimeException( "Encountered unrecognized profiling-related files\n\n+" +
                                        errorMessage );
        }
    }

    private static boolean hasRecognizedSuffix( Path file )
    {
        return Arrays.stream( RecordingType.values() ).anyMatch( recordingType -> file.getFileName().toString().endsWith( recordingType.extension() ) );
    }
}
