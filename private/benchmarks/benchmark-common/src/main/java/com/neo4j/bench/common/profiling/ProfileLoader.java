/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor.tryParse;
import static java.lang.String.format;

public class ProfileLoader
{
    private static final Logger LOG = LoggerFactory.getLogger( ProfileLoader.class );

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
                for ( ProfilerType profilerType : ProfilerType.values() )
                {
                    for ( RecordingType recordingType : profilerType.allRecordingTypes() )
                    {
                        ProfilerRecordingDescriptor.ParseResult parseResult = tryParse( filename,
                                                                                        recordingType,
                                                                                        benchmarkGroupBenchmark.benchmarkGroup(),
                                                                                        benchmarkGroupBenchmark.benchmark(),
                                                                                        RunPhase.MEASUREMENT );
                        if ( parseResult.isMatch() )
                        {
                            Parameters additionalParameters = parseResult.additionalParameters();
                            ProfilerRecordings profilerRecordings = benchmarkProfiles.computeIfAbsent(
                                    benchmarkGroupBenchmark,
                                    bgb -> new ProfilerRecordings() );
                            benchmarkProfiles.put(
                                    benchmarkGroupBenchmark,
                                    profilerRecordings.with( recordingType, additionalParameters, s3Bucket + filename ) );
                            found = true;

                            if ( parseResult.match() == ProfilerRecordingDescriptor.Match.IS_SANITIZED )
                            {
                                errors.add( format( "WARNING: Found un-sanitized profiler recording filename\n" +
                                                    "\tFilename: %s", filename ) );
                            }
                            break;
                        }
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

        if ( errors.isEmpty() || ignoreUnrecognizedFiles )
        {
            errors.forEach( LOG::debug );
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
