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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ForkDirectory
{
    private static final String PLAN_JSON = "plan.json";
    private final Path dir;

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
            ForkDescription forkDescription = new ForkDescription( name, dir );
            forkDescription.save();
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
        return new ForkDirectory( dir );
    }

    private ForkDirectory( Path dir )
    {
        this.dir = dir;
    }

    public String name()
    {
        return ForkDescription.load( dir ).name();
    }

    public String toAbsolutePath()
    {
        return dir.toAbsolutePath().toString();
    }

    public Map<RecordingDescriptor,Path> copyProfilerRecordings( Path targetDir, Predicate<RecordingDescriptor> doCopyPredicate )
    {
        Map<RecordingDescriptor,Path> recordings = ForkDescription.load( dir ).recordings;
        Map<Path,RecordingDescriptor> validRecordings = recordings.keySet().stream()
                                                                  .filter( descriptor -> descriptor.runPhase().equals( RunPhase.MEASUREMENT ) )
                                                                  .collect( toMap( recordings::get, identity() ) );

        try ( Stream<Path> files = Files.walk( dir ) )
        {
            return files.filter( validRecordings::containsKey )
                        .map( recording -> findRecording( recording, validRecordings ) )
                        .filter( doCopyPredicate )
                        .collect( Collectors.toMap( Function.identity(), recordingDescriptor -> copy( targetDir, recordingDescriptor ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error copying profile recordings\n" +
                                                    "From : %s\n" +
                                                    "To   : %s", dir, targetDir ), e );
        }
    }

    private Path copy( Path targetDir, RecordingDescriptor recordingDescriptor )
    {
        Path relativePath = copyFile( recordingDescriptor, targetDir );
        writeParamsDescription( recordingDescriptor, targetDir );
        return relativePath;
    }

    private RecordingDescriptor findRecording( Path recording, Map<Path,RecordingDescriptor> validRecordings )
    {
        RecordingDescriptor recordingDescriptor = validRecordings.get( recording );
        String filename = recording.getFileName().toString();
        if ( !filename.equals( recordingDescriptor.sanitizedFilename() ) )
        {
            throw new IllegalStateException( format( "Encountered unexpected profiler recording filename\n" +
                                                     "Expected name : %s\n" +
                                                     "Actual name   : %s",
                                                     recordingDescriptor.filename(),
                                                     recording.toAbsolutePath().toString() ) );
        }
        return recordingDescriptor;
    }

    private Path copyFile( RecordingDescriptor recordingDescriptor, Path toPath )
    {
        Path source = pathFor( recordingDescriptor.sanitizedFilename() );
        Path relativeTarget = Paths.get( recordingDescriptor.filename() );
        Path target = toPath.resolve( relativeTarget );
        try
        {
            if ( recordingDescriptor.isDuplicatesAllowed() )
            {
                // Some recording types are produced by every fork, there will be duplicates (in different forks) with the same name
                 Files.copy( source, target, StandardCopyOption.REPLACE_EXISTING );
            }
            else
            {
                Files.copy( source, target );
            }
            return relativeTarget;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error copying profile recording\n" +
                                                    "From : %s\n" +
                                                    "To   : %s", source, target ), e );
        }
    }

    private void writeParamsDescription( RecordingDescriptor recordingDescriptor, Path toPath )
    {
        Path target = toPath.resolve( recordingDescriptor.paramsFilename() );
        try
        {
            String parametersDescription = recordingDescriptor.name();
            Files.write( target, parametersDescription.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error writing parameters description: %s", target ), e );
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
        ForkDescription forkDescription = ForkDescription.load( dir );
        forkDescription.registerRecording( recordingDescriptor, path );
        forkDescription.save();
        return path;
    }

    public Path findRegisteredPathFor( RecordingDescriptor recordingDescriptor )
    {
        Path path = pathFor( recordingDescriptor.sanitizedFilename() );
        if ( !ForkDescription.load( dir ).recordings.containsKey( recordingDescriptor ) )
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
        return ForkDescription.load( dir ).recordings;
    }

    boolean isMeasurementFork()
    {
        return !isProfiledFork();
    }

    private boolean isProfiledFork()
    {
        ForkDescription forkDescription = ForkDescription.load( dir );
        return forkDescription.recordings.keySet().stream()
                                         .anyMatch( recordingDescriptor -> !recordingDescriptor.recordingType().equals( RecordingType.NONE ) &&
                                                                           !recordingDescriptor.recordingType().equals( RecordingType.HEAP_DUMP ) &&
                                                                           !recordingDescriptor.recordingType().equals( RecordingType.ASCII_PLAN ) );
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
        private static final String FORK_JSON = "fork.json";

        private final String name;
        private final Path root;
        @JsonSerialize( keyUsing = RecordingDescriptor.RecordingDescriptorKeySerializer.class )
        @JsonDeserialize( keyUsing = RecordingDescriptor.RecordingDescriptorKeyDeserializer.class )
        private final Map<RecordingDescriptor,Path> recordings;

        private static ForkDescription load( Path dir )
        {
            Path jsonPath = dir.resolve( FORK_JSON );
            assertFileExists( jsonPath );
            ForkDescription fork = JsonUtil.deserializeJson( jsonPath, ForkDescription.class );
            Map<RecordingDescriptor,Path> rebasedRecordings = rebase( fork.root, dir, fork.recordings );
            return new ForkDescription( fork.name, dir, rebasedRecordings );
        }

        /**
         * Jackson saves all paths as absolute, so we want to reverse it and keep them relative to {@link ForkDirectory#dir}.
         */
        private static Map<RecordingDescriptor,Path> rebase( Path fromDir, Path toDir, Map<RecordingDescriptor,Path> recordings )
        {
            return recordings.entrySet()
                             .stream()
                             .collect( toMap( Map.Entry::getKey, entry -> toDir.resolve( fromDir.relativize( entry.getValue() ) ) ) );
        }

        @JsonCreator
        private ForkDescription( @JsonProperty( "name" ) String name, @JsonProperty( "root" ) Path root )
        {
            this( name, root, new HashMap<>() );
        }

        private ForkDescription( String name, Path root, Map<RecordingDescriptor,Path> recordings )
        {
            this.name = name;
            this.root = root;
            this.recordings = recordings;
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

        private void save()
        {
            Path jsonPath = pathFor( root, FORK_JSON );
            JsonUtil.serializeJson( jsonPath, this );
        }
    }
}
