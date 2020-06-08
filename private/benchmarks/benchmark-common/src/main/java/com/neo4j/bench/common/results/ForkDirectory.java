/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor.Match.IS_SANITIZED;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static java.lang.String.format;

public class ForkDirectory
{
    private static final String FORK_JSON = "fork.json";
    public static final String PLAN_JSON = "plan.json";
    private final Path dir;
    private final ForkDescription forkDescription;

    public static ForkDirectory findOrFailAt( Path parentDir, String name )
    {
        Path forkDir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
        return openAt( forkDir );
    }

    public static ForkDirectory findOrCreateAt( Path parentDir, String name, List<ParameterizedProfiler> profilers )
    {
        Path dir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
        if ( Files.exists( dir ) )
        {
            Path jsonPath = pathFor( dir, FORK_JSON );
            updateForkDetails( jsonPath, profilers );
            return openAt( dir );
        }
        else
        {
            return createAt( parentDir, name, profilers );
        }
    }

    public static ForkDirectory createAt( Path parentDir, String name, List<ParameterizedProfiler> profilers )
    {
        try
        {
            Path dir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
            assertDoesNotExist( dir );
            Files.createDirectory( dir );
            saveForkDetails( dir, name, profilers );
            return openAt( dir );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error creating result directory for fork '%s' in %s", name, parentDir.toAbsolutePath() ), e );
        }
    }

    public static ForkDirectory openAt( Path dir )
    {
        assertDirectoryExists( dir );
        assertFileExists( dir.resolve( FORK_JSON ) );
        return new ForkDirectory( dir );
    }

    private static void saveForkDetails( Path dir, String name, List<ParameterizedProfiler> profilers )
    {
        Path jsonPath = create( dir, FORK_JSON );
        ForkDescription forkDescription = new ForkDescription( name );
        forkDescription.addProfilers( profilers );
        JsonUtil.serializeJson( jsonPath, forkDescription );
    }

    private static void updateForkDetails( Path jsonPath, List<ParameterizedProfiler> profilers )
    {
        ForkDescription forkDescription = loadDescription( jsonPath );
        forkDescription.addProfilers( profilers );
        JsonUtil.serializeJson( jsonPath, forkDescription );
    }

    private ForkDirectory( Path dir )
    {
        this.dir = dir;
        this.forkDescription = loadDescription( dir.resolve( FORK_JSON ) );
    }

    public void unsanitizeProfilerRecordingsFor( BenchmarkGroup benchmarkGroup,
                                                 Benchmark benchmark,
                                                 ProfilerType profilerType,
                                                 Parameters additionalParameters )
    {
        for ( RunPhase runPhase : RunPhase.values() )
        {
            ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                                                                  benchmark,
                                                                                                  runPhase,
                                                                                                  profilerType,
                                                                                                  additionalParameters );
            unsanitizeProfilerRecordingsFor( recordingDescriptor );
        }
    }

    private void unsanitizeProfilerRecordingsFor( ProfilerRecordingDescriptor recordingDescriptor )
    {
        try
        {
            for ( RecordingType recordingType : recordingDescriptor.recordingTypes() )
            {
                Path sanitizedFile = pathFor( recordingDescriptor.sanitizedFilename( recordingType ) );
                if ( Files.exists( sanitizedFile ) )
                {
                    Files.move( sanitizedFile,
                                pathFor( recordingDescriptor.filename( recordingType ) ),
                                StandardCopyOption.REPLACE_EXISTING );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error while try to rename recordings for:\n" + recordingDescriptor, e );
        }
    }

    public String name()
    {
        return forkDescription.name();
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    public void copyProfilerRecordings( BenchmarkGroup benchmarkGroup, Benchmark benchmark, Path targetDir, Set<RecordingType> excluding )
    {
        try
        {
            // copy all profiler recordings, except those that are explicitly excluded
            List<RecordingType> recordingTypes = profilers().stream()
                                                            .flatMap( profilerType -> profilerType.allRecordingTypes().stream() )
                                                            .filter( recordingType -> !excluding.contains( recordingType ) )
                                                            .collect( Collectors.toList() );

            PathMatcher pathMatcher = path ->
            {
                for ( RecordingType recordingType : recordingTypes )
                {
                    ProfilerRecordingDescriptor.ParseResult parsedRecording = ProfilerRecordingDescriptor.tryParse( path.getFileName().toString(),
                                                                                                                    recordingType,
                                                                                                                    benchmarkGroup,
                                                                                                                    benchmark,
                                                                                                                    RunPhase.MEASUREMENT );
                    if ( parsedRecording.isMatch() )
                    {
                        return true;
                    }
                    else if ( parsedRecording.match().equals( IS_SANITIZED ) )
                    {
                        throw new RuntimeException( "Found an un-sanitized profiler recording: " + path.toAbsolutePath() );
                    }
                }
                return false;
            };

            Files.walkFileTree( dir, new CopyMatchedFilesVisitor( targetDir, pathMatcher ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error copying profile recordings\n" +
                                                    "From : %s\n" +
                                                    "To   : %s", dir, targetDir ),
                                            e );
        }
    }

    private static class CopyMatchedFilesVisitor extends SimpleFileVisitor<Path>
    {
        private final Path toPath;
        private final PathMatcher matcher;

        private CopyMatchedFilesVisitor( Path toPath, PathMatcher matcher )
        {
            this.toPath = toPath;
            this.matcher = matcher;
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs )
        {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
        {
            if ( matcher.matches( file ) )
            {
                Files.copy( file, toPath.resolve( file.getFileName() ) );
            }
            return FileVisitResult.CONTINUE;
        }
    }

    public Path pathForPlan()
    {
        return pathFor( PLAN_JSON );
    }

    public Path findOrCreate( String filename )
    {
        Path file = pathFor( filename );
        return Files.exists( file ) ? file : create( filename );
    }

    public Path findOrFail( String filename )
    {
        Path file = pathFor( filename );
        if ( Files.exists( file ) )
        {
            return file;
        }
        else
        {
            throw new RuntimeException( format( "No files named '%s' found in: %s", filename, dir.toAbsolutePath().toString() ) );
        }
    }

    public Path create( String filename )
    {
        return create( dir, filename );
    }

    public Path pathFor( ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return pathFor( profilerRecordingDescriptor.sanitizedFilename() );
    }

    public Path pathFor( String filename )
    {
        return pathFor( dir, filename );
    }

    public Path logError( Throwable e )
    {
        try
        {
            String exceptionString = ErrorReporter.stackTraceToString( e );
            Path errorLog = newErrorLog();
            return Files.write( errorLog, exceptionString.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException ioe )
        {
            throw new UncheckedIOException( ioe );
        }
    }

    public Path newErrorLog()
    {
        return dir.resolve( "error-" + UUID.randomUUID() + ".log" );
    }

    public Set<ProfilerType> profilers()
    {
        return forkDescription.parameterizedProfilers().stream().map( ParameterizedProfiler::profilerType ).collect( Collectors.toSet() );
    }

    private static ForkDescription loadDescription( Path jsonPath )
    {
        assertFileExists( jsonPath );
        return JsonUtil.deserializeJson( jsonPath, ForkDescription.class );
    }

    private static Path create( Path dir, String filename )
    {
        try
        {
            Path file = pathFor( dir, filename );
            // check that we are not aware of this file already
            assertFileDoesNotExist( file );
            // try to create
            return Files.createFile( file );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error creating: " + filename, e );
        }
    }

    private static Path pathFor( Path dir, String filename )
    {
        return dir.resolve( filename );
    }

    private static void assertFileDoesNotExist( Path file )
    {
        if ( Files.exists( file ) )
        {
            throw new RuntimeException( "File already exists: " + file.toAbsolutePath() );
        }
    }

    private static class ForkDescription
    {
        private final String name;
        private final Set<ParameterizedProfiler> parameterizedProfilers;

        private ForkDescription()
        {
            this( "INVALID_NAME" );
        }

        private ForkDescription( String name )
        {
            this.name = name;
            this.parameterizedProfilers = new HashSet<>();
        }

        private String name()
        {
            if ( "INVALID_NAME".equals( name ) )
            {
                throw new RuntimeException( "Object was deserialized incorrectly" );
            }
            return name;
        }

        private Set<ParameterizedProfiler> parameterizedProfilers()
        {
            return parameterizedProfilers;
        }

        private void addProfilers( Collection<ParameterizedProfiler> newProfilers )
        {
            this.parameterizedProfilers.addAll( newProfilers );
        }
    }
}
