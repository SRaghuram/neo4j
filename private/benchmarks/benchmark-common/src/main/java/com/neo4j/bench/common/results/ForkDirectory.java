/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ForkDirectory
{
    private static final String FORK_JSON = "fork.json";
    private static final String PLAN_JSON = "plan.json";
    private final Path dir;
    private final ForkDescription forkDescription;

    static ForkDirectory findOrFailAt( Path parentDir, String name )
    {
        Path forkDir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
        return openAt( forkDir );
    }

    static ForkDirectory findOrCreateAt( Path parentDir, String name )
    {
        Path dir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
        if ( Files.exists( dir ) )
        {
            return openAt( dir );
        }
        else
        {
            return createAt( parentDir, name );
        }
    }

    static ForkDirectory createAt( Path parentDir, String name )
    {
        try
        {
            Path dir = parentDir.resolve( BenchmarkUtil.sanitize( name ) );
            assertDoesNotExist( dir );
            Files.createDirectory( dir );
            saveForkDetails( dir, name );
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

    private static void saveForkDetails( Path dir, String name )
    {
        Path jsonPath = create( dir, FORK_JSON );
        ForkDescription forkDescription = new ForkDescription( name );
        JsonUtil.serializeJson( jsonPath, forkDescription );
    }

    private ForkDirectory( Path dir )
    {
        this.dir = dir;
        this.forkDescription = loadDescription( dir.resolve( FORK_JSON ) );
    }

    public String name()
    {
        return forkDescription.name();
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    public void copyProfilerRecordings( Path targetDir,
                                        Predicate<RecordingDescriptor> doCopyPredicate,
                                        BiConsumer<RecordingDescriptor,Path> onCopyConsumer )
    {
        try
        {
            Map<RecordingDescriptor,Path> recordings = forkDescription.recordings;
            Map<Path,RecordingDescriptor> validRecordings = recordings.keySet().stream()
                                                                      .filter( descriptor -> descriptor.runPhase().equals( RunPhase.MEASUREMENT ) )
                                                                      .collect( toMap( recordings::get, identity() ) );

            Files.walkFileTree( dir,
                                new CopyMatchedFilesVisitor( targetDir, new SanitizingPathMatcher( validRecordings, doCopyPredicate, onCopyConsumer ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error copying profile recordings\n" +
                                                    "From : %s\n" +
                                                    "To   : %s", dir, targetDir ),
                                            e );
        }
    }

    private static class SanitizingPathMatcher implements PathMatcher
    {
        private final Map<Path,RecordingDescriptor> validRecordings;
        private final Predicate<RecordingDescriptor> doCopyPredicate;
        private final BiConsumer<RecordingDescriptor,Path> onCopyConsumer;

        private SanitizingPathMatcher( Map<Path,RecordingDescriptor> validRecordings,
                                       Predicate<RecordingDescriptor> doCopyPredicate,
                                       BiConsumer<RecordingDescriptor,Path> onCopyConsumer )
        {
            this.validRecordings = validRecordings;
            this.doCopyPredicate = doCopyPredicate;
            this.onCopyConsumer = onCopyConsumer;
        }

        @Override
        public boolean matches( Path recording )
        {
            boolean doCopy = validRecordings.containsKey( recording ) &&
                             doCopyPredicate.test( validRecordings.get( recording ) );
            if ( doCopy )
            {
                Path deSanitizedRecording = Paths.get( deSanitizedName( recording ) );
                onCopyConsumer.accept( validRecordings.get( recording ), deSanitizedRecording );
            }
            return doCopy;
        }

        private String deSanitizedName( Path recording )
        {
            RecordingDescriptor recordingDescriptor = validRecordings.get( recording );
            if ( null == recordingDescriptor )
            {
                throw new IllegalStateException( format( "Tried to de-sanitize a file that has no associated recording descriptor\n" +
                                                         "File: %s",
                                                         recording.toAbsolutePath() ) );
            }
            String filename = recording.getFileName().toString();
            if ( filename.equals( recordingDescriptor.sanitizedFilename() ) )
            {
                return recordingDescriptor.filename();
            }
            else
            {
                throw new IllegalStateException( format( "Encountered unexpected profiler recording filename\n" +
                                                         "Expected name : %s\n" +
                                                         "Actual name   : %s",
                                                         recordingDescriptor.filename(),
                                                         recording.toAbsolutePath().toString() ) );
            }
        }
    }

    private static class CopyMatchedFilesVisitor extends SimpleFileVisitor<Path>
    {
        private final Path toPath;
        private final SanitizingPathMatcher matcher;

        private CopyMatchedFilesVisitor( Path toPath, SanitizingPathMatcher matcher )
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
                Files.copy( file, toPath.resolve( matcher.deSanitizedName( file ) ) );
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

    public Path registerPathFor( RecordingDescriptor recordingDescriptor )
    {
        Path path = pathFor( recordingDescriptor.sanitizedFilename() );
        forkDescription.registerRecording( recordingDescriptor, path );
        Path jsonPath = pathFor( dir, FORK_JSON );
        JsonUtil.serializeJson( jsonPath, forkDescription );
        return path;
    }

    public Path findRegisteredPathFor( RecordingDescriptor recordingDescriptor )
    {
        Path path = pathFor( recordingDescriptor.sanitizedFilename() );
        if ( !forkDescription.recordings.containsKey( recordingDescriptor ) )
        {
            throw new IllegalStateException( "Fork directory does not have a registered recording for: " + recordingDescriptor );
        }
        return path;
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

    public Map<RecordingDescriptor,Path> recordings()
    {
        return forkDescription.recordings;
    }

    boolean isMeasurementFork()
    {
        return !isProfiledFork();
    }

    private boolean isProfiledFork()
    {
        return forkDescription.recordings.keySet().stream()
                                         .anyMatch( recordingDescriptor -> !recordingDescriptor.recordingType().equals( RecordingType.NONE ) &&
                                                                           !recordingDescriptor.recordingType().equals( RecordingType.HEAP_DUMP ) );
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
        private String name;

        @JsonSerialize( keyUsing = RecordingDescriptor.RecordingDescriptorKeySerializer.class )
        @JsonDeserialize( keyUsing = RecordingDescriptor.RecordingDescriptorKeyDeserializer.class )
        private Map<RecordingDescriptor,Path> recordings;

        @JsonCreator
        private ForkDescription( @JsonProperty( "name" ) String name )
        {
            this.name = name;
            this.recordings = new HashMap<>();
        }

        private String name()
        {
            return name;
        }

        private void registerRecording( RecordingDescriptor recordingDescriptor, Path recordingFile )
        {
            if ( recordings.containsValue( recordingFile ) )
            {
                throw new IllegalStateException( format( "Recording file has already been registered: %s", recordingFile.toAbsolutePath() ) );
            }
            recordings.put( recordingDescriptor, recordingFile );
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }
}
