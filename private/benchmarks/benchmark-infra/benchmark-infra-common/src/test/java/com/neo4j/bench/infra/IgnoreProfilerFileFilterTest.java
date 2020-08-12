/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.model.profiling.RecordingType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class IgnoreProfilerFileFilterTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldFilerOutProfilingFiles() throws IOException
    {
        Path topFolder = temporaryFolder.newFolder().toPath();
        RecordingType[] recordingTypes = RecordingType.values();
        Arrays.stream( recordingTypes ).forEach( recordingType ->
                                                 {
                                                     try
                                                     {
                                                         Files.createFile( topFolder.resolve( "shouldBeFiltered" + recordingType.extension() ) );
                                                     }
                                                     catch ( IOException e )
                                                     {
                                                         throw new UncheckedIOException( e );
                                                     }
                                                 } );
        Path shouldSave = Files.createFile( topFolder.resolve( "shouldNotBeFiltered.txt" ) );
        IgnoreProfilerFileFilter ignoreProfilerFileFilter = new IgnoreProfilerFileFilter();
        try ( Stream<Path> dirs = Files.walk( topFolder ) )
        {
            Set<Path> filteredPaths = dirs.filter( dir -> ignoreProfilerFileFilter.accept( dir.toFile() ) ).collect( Collectors.toSet() );
            //Should contains both the file and the folder
            assertThat( filteredPaths, containsInAnyOrder( shouldSave, topFolder ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Encountered error while traversing work directory", e );
        }
    }
}
