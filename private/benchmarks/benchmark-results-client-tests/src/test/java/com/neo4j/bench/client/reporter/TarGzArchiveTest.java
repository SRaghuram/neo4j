/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class TarGzArchiveTest
{

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    public void createEmptyArchive() throws Exception
    {
        File archiveDir = temporaryFolder.directory( "archive" ).toFile();
        File extractDir = temporaryFolder.directory( "extract" ).toFile();

        File archiveFile = temporaryFolder.file( "archive.tar.gz" ).toFile();
        // when
        TarGzArchive.compress( archiveFile.toPath(), archiveDir.toPath() );
        // then
        TarGzArchive.extract( archiveFile.toPath(), extractDir.toPath() );
        assertEquals( 0, extractDir.listFiles().length );
    }

    @Test
    public void createArchiveWithOneFile() throws Exception
    {
        // given
        File archiveDir = temporaryFolder.directory( "archive" ).toFile();
        File extractDir = temporaryFolder.directory( "extract" ).toFile();

        Files.write( archiveDir.toPath().resolve( "file" ), Arrays.asList( "file" ) );
        File archiveFile = temporaryFolder.file( "archive.tar.gz" ).toFile();
        // when
        TarGzArchive.compress( archiveFile.toPath(), archiveDir.toPath() );
        // then
        TarGzArchive.extract( archiveFile.toPath(), extractDir.toPath() );
        assertEquals( 1, extractDir.listFiles().length );
        Path file = extractDir.toPath().resolve( "file" );
        assertTrue( Files.isRegularFile( file ) );
        assertEquals( "file\n", new String( Files.readAllBytes( file ) ) );
    }

    @Test
    public void createArchiveWithOneEmptyDirectory() throws Exception
    {
        // given
        File archiveDir = temporaryFolder.directory( "archive" ).toFile();
        File extractDir = temporaryFolder.directory( "extract" ).toFile();

        Files.createDirectories( archiveDir.toPath().resolve( "folder" ) );
        File archiveFile = temporaryFolder.file( "archive.tar.gz" ).toFile();
        // when
        TarGzArchive.compress( archiveFile.toPath(), archiveDir.toPath() );
        // then
        TarGzArchive.extract( archiveFile.toPath(), extractDir.toPath() );
        assertEquals( 1, extractDir.listFiles().length );
        Path folder = extractDir.toPath().resolve( "folder" );
        assertTrue( Files.isDirectory( folder ) );
        assertEquals( 0, folder.toFile().list().length );
    }
}
