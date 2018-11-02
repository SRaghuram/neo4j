/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupCopyServiceTest
{
    private FileMoveProvider fileMoveProvider;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    BackupCopyService subject;

    @Before
    public void setup()
    {
        PageCache pageCache = mock( PageCache.class );
        fileMoveProvider = mock( FileMoveProvider.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        subject = new BackupCopyService( fs, fileMoveProvider );
    }

    @Test
    public void logicForMovingBackupsIsDelegatedToFileMovePropagator() throws IOException
    {
        // given
        Path parentDirectory = testDirectory.directory( "parent" ).toPath();
        Path oldLocation = parentDirectory.resolve( "oldLocation" );
        Files.createDirectories( oldLocation );
        Path newLocation = parentDirectory.resolve( "newLocation" );

        // and
        FileMoveAction fileOneMoveAction = mock( FileMoveAction.class );
        FileMoveAction fileTwoMoveAction = mock( FileMoveAction.class );
        when( fileMoveProvider.traverseForMoving( any() ) ).thenReturn( Stream.of( fileOneMoveAction, fileTwoMoveAction ) );

        // when
        subject.moveBackupLocation( oldLocation, newLocation );

        // then file move propagator was requested with correct source and baseDirectory
        verify( fileMoveProvider ).traverseForMoving( oldLocation.toFile() );

        // and files were moved to correct target directory
        verify( fileOneMoveAction ).move( newLocation.toFile() );
        verify( fileTwoMoveAction ).move( newLocation.toFile() );
    }
}
