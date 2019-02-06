/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.results;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.neo4j.bench.client.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.client.util.BenchmarkUtil.assertFileExists;

import static java.lang.String.format;

public class ForkDirectory
{
    private static final String FORK_JSON = "fork.json";
    static final String PLAN_JSON = "plan.json";
    private final Path dir;
    private final ForkDescription forkDescription;

    static ForkDirectory createAt( Path parentDir, String name, List<ProfilerType> profilers )
    {
        try
        {
            Path dir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
            assertDoesNotExist( dir );
            Files.createDirectory( dir );
            serializeForkDetails( dir, name, profilers );
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

    private static void serializeForkDetails( Path dir, String name, List<ProfilerType> profilers )
    {
        Path jsonPath = create( dir, FORK_JSON );
        JsonUtil.serializeJson( jsonPath, new ForkDescription( name, profilers ) );
    }

    private ForkDirectory( Path dir )
    {
        this.dir = dir;
        this.forkDescription = loadDescription( dir );
    }

    public String name()
    {
        return forkDescription.name();
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    void copyProfilerRecordings( BenchmarkGroup benchmarkGroup, Benchmark benchmark, Path targetDir, Set<RecordingType> excluding )
    {
        Set<Path> validRecordingFiles = new HashSet<>();
        for ( ProfilerType profilerType : profilers() )
        {
            ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, RunPhase.MEASUREMENT, profilerType );
            // copy all profiler recordings, except those that are explicitly excluded
            List<RecordingType> recordingTypes = profilerType.allRecordingTypes()
                                                             .stream()
                                                             .filter( recordingType -> !excluding.contains( recordingType ) )
                                                             .collect( Collectors.toList() );
            for ( RecordingType recordingType : recordingTypes )
            {
                String recordingFilename = recordingDescriptor.filename( recordingType );
                Path validRecordingFile = dir.resolve( recordingFilename );
                validRecordingFiles.add( validRecordingFile );
            }
        }
        try
        {
            Files.walkFileTree( dir, new CopyMatchedFilesVisitor( targetDir, validRecordingFiles::contains ) );
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
        return pathFor( profilerRecordingDescriptor.filename() );
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
            Path errorFile = dir.resolve( "error-" + System.currentTimeMillis() + ".log" );
            return Files.write( errorFile, exceptionString.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException ioe )
        {
            throw new UncheckedIOException( ioe );
        }
    }

    public List<ProfilerType> profilers()
    {
        return forkDescription.profilers();
    }

    private static ForkDescription loadDescription( Path dir )
    {
        Path jsonPath = dir.resolve( FORK_JSON );
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
        private final List<ProfilerType> profilers;

        private ForkDescription()
        {
            this( "INVALID_NAME", new ArrayList<>() );
        }

        private ForkDescription( String name, List<ProfilerType> profilers )
        {
            this.name = name;
            this.profilers = profilers;
        }

        private String name()
        {
            if ( "INVALID_NAME".equals( name ) )
            {
                throw new RuntimeException( "Object was deserialized incorrectly" );
            }
            return name;
        }

        public List<ProfilerType> profilers()
        {
            return profilers;
        }
    }
}
