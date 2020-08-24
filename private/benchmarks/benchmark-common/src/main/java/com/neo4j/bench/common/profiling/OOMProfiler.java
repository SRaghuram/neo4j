/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class OOMProfiler implements ExternalProfiler
{
    private static final Logger LOG = LoggerFactory.getLogger( OOMProfiler.class );

    @Override
    public List<String> invokeArgs(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs(
            JvmVersion jvmVersion,
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor,
            Resources resources )
    {
        Path oomScript = findOnOutOfMemoryScript( resources );
        Path oomDirectory = createOOMDirectory( forkDirectory );
        // create parameters
        return JvmArgs.from(
                format( "-XX:OnOutOfMemoryError=%s --jvm-pid %%p --output-dir %s", oomScript, oomDirectory ),
                "-XX:+HeapDumpOnOutOfMemoryError",
                format( "-XX:HeapDumpPath=%s", oomDirectory ) );
    }

    @Override
    public void beforeProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
    }

    @Override
    public void afterProcess(
            ForkDirectory forkDirectory,
            ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        try
        {
            RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.HEAP_DUMP );
            Files.createFile( forkDirectory.registerPathFor( recordingDescriptor ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "failed to create empty heap dump", e );
        }
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        Path oomDirectory = getOOMDirectory( forkDirectory );

        try ( Stream<Path> paths = Files.list( oomDirectory ) )
        {
            List<Path> allHeapDumps = paths.filter( Files::isRegularFile )
                                           .filter( path -> path.getFileName().toString().endsWith( RecordingType.HEAP_DUMP.extension() ) )
                                           .collect( Collectors.toList() );

            if ( allHeapDumps.size() == 1 )
            {
                RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.HEAP_DUMP );
                Files.move( allHeapDumps.get( 0 ), forkDirectory.registerPathFor( recordingDescriptor ) );
            }
            else if ( allHeapDumps.size() == 0 )
            {
                LOG.debug( "no heap dump recorded" );
            }
            else
            {
                throw new RuntimeException( "too many heap dumps recorded" );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "failed to find heap dumps", e );
        }
    }

    private Path createOOMDirectory( ForkDirectory forkDirectory )
    {
        Path oomDirectory = getOOMDirectory( forkDirectory );
        try
        {
            LOG.debug( format( "creating OOM directory at %s", oomDirectory ) );
            Files.createDirectories( oomDirectory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "cannot create out of memory dump directory at %s", oomDirectory ), e );
        }
        return oomDirectory;
    }

    private Path findOnOutOfMemoryScript( Resources resources )
    {
        Path onOutOfMemoryScript = resources.getResourceFile( "/bench/profiling/on-out-of-memory.sh" );
        assertIsExecutable( onOutOfMemoryScript );
        return onOutOfMemoryScript;
    }

    private static void assertIsExecutable( Path onOutOfMemoryScript )
    {
        try
        {
            Files.setPosixFilePermissions( onOutOfMemoryScript, ImmutableSet.of( PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE ) );
        }
        catch ( UnsupportedOperationException e )
        {
            // no op, can happen on Windows, and its fine
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "cannot update file %s permissions", onOutOfMemoryScript ), e );
        }
    }

    // for testing
    static Path getOOMDirectory( ForkDirectory forkDirectory )
    {
        return Paths.get( forkDirectory.toAbsolutePath() ).resolve( "out-of-memory" );
    }
}
