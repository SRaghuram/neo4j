/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdFilesDeleterTest
{
    @Rule
    public final FileSystemRule fsRule = new DefaultFileSystemRule();
    private final FileSystemAbstraction fs = fsRule.get();

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldReturnFalseWhenNoIdFilesDeleted()
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.directory() );

        // when
        boolean anyIdFilesDeleted = IdFilesDeleter.deleteIdFiles( databaseLayout, fs );

        // then
        assertFalse( anyIdFilesDeleted );
    }

    @Test
    public void shouldDeleteIdFiles() throws IOException
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.directory() );

        for ( File idFile : databaseLayout.idFiles() )
        {
            testDirectory.createFile( idFile.getName() );
        }

        // when
        boolean anyIdFilesDeleted = IdFilesDeleter.deleteIdFiles( databaseLayout, fs );

        // then
        assertTrue( anyIdFilesDeleted );
    }

    @Test
    public void shouldNotDeleteUnknownFiles() throws IOException
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.directory() );

        File unknownA = testDirectory.createFile( "unknown" );
        File unknownB = testDirectory.createFile( "unknown.id" );
        String nodeIdFileName = databaseLayout.idFile( DatabaseFile.NODE_STORE ).orElseThrow( IllegalStateException::new ).getName();
        File known = testDirectory.createFile( nodeIdFileName );

        // when
        boolean anyIdFilesDeleted = IdFilesDeleter.deleteIdFiles( databaseLayout, fs );

        // then
        assertTrue( anyIdFilesDeleted );
        assertFalse( fs.fileExists( known ) );
        assertTrue( fs.fileExists( unknownA ) );
        assertTrue( fs.fileExists( unknownB ) );
    }
}
