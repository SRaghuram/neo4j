/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FileNamesTest
{
    @Test
    void shouldProperlyFormatFilenameForVersion()
    {
        // Given
        Path base = Path.of( "base" );
        FileNames fileNames = new FileNames( base );

        // When - Then
        // when asking for a given version...
        for ( int i = 0; i < 100; i++ )
        {
            Path forVersion = fileNames.getForSegment( i );
            // ...then the expected thing is returned
            assertEquals( forVersion, base.resolve( FileNames.BASE_FILE_NAME + i ) );
        }
    }

    @Test
    void shouldWorkCorrectlyOnReasonableDirectoryContents()
    {
        // Given
        // a raft log directory with just the expected files, without gaps
        Path base = Path.of( "base" );
        FileNames fileNames = new FileNames( base );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        Log log = mock( Log.class );
        List<File> filesPresent = new LinkedList<>();
        int lower = 0;
        int upper = 24;
        // the files are added in reverse order, so we can verify that FileNames orders based on version
        for ( int i = upper; i >= lower; i-- )
        {
            filesPresent.add( fileNames.getForSegment( i ).toFile() );
        }
        when( fsa.listFiles( base.toFile() ) ).thenReturn( filesPresent.toArray( new File[]{} ) );

        // When
        // asked for the contents of the directory
        SortedMap<Long, Path> allFiles = fileNames.getAllFiles( fsa, log );

        // Then
        // all the things we added above should be returned
        assertEquals( upper - lower + 1, allFiles.size() );
        long currentVersion = lower;
        for ( Map.Entry<Long, Path> longFileEntry : allFiles.entrySet() )
        {
            assertEquals( currentVersion, longFileEntry.getKey().longValue() );
            assertEquals( fileNames.getForSegment( currentVersion ), longFileEntry.getValue() );
            currentVersion++;
        }
    }

    @Test
    void shouldIgnoreUnexpectedLogDirectoryContents()
    {
        // Given
        // a raft log directory with just the expected files, without gaps
        Path base = Path.of( "base" );
        FileNames fileNames = new FileNames( base );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        Log log = mock( Log.class );
        List<File> filesPresent = new LinkedList<>();

        filesPresent.add( fileNames.getForSegment( 0 ).toFile() ); // should be included
        filesPresent.add( fileNames.getForSegment( 1 ).toFile() ); // should be included
        filesPresent.add( fileNames.getForSegment( 10 ).toFile() ); // should be included
        filesPresent.add( fileNames.getForSegment( 11 ).toFile() ); // should be included
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "01" ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "001" ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "-1" ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "1a" ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "a1" ).toFile() ); // should be ignored
        filesPresent.add( base.resolve( FileNames.BASE_FILE_NAME + "ab" ).toFile() ); // should be ignored

        when( fsa.listFiles( base.toFile() ) ).thenReturn( filesPresent.toArray( new File[]{} ) );

        // When
        // asked for the contents of the directory
        SortedMap<Long,Path> allFiles = fileNames.getAllFiles( fsa, log );

        // Then
        // only valid things should be returned
        assertEquals( 4, allFiles.size() );
        assertEquals( allFiles.get( 0L ), fileNames.getForSegment( 0 ) );
        assertEquals( allFiles.get( 1L ), fileNames.getForSegment( 1 ) );
        assertEquals( allFiles.get( 10L ), fileNames.getForSegment( 10 ) );
        assertEquals( allFiles.get( 11L ), fileNames.getForSegment( 11 ) );

        // and the invalid ones should be logged
        verify( log, times( 7 ) ).warn( anyString() );
    }
}
