/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class MetadataStoreTest
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private MetadataStore metadataStore;

    @BeforeEach
    void setup()
    {
        metadataStore = new MetadataStore( fs );
    }

    @Test
    public void metadataStoreShouldWriteDataToFile() throws IOException
    {
        final var folder = testDirectory.directory( "folder1" );
        final var elements = List.of( "a", "b" );

        //when
        metadataStore.write( folder, elements );

        //then
        assertEquals( List.of("a;","b;"), readLines( MetadataStore.getFilePath( folder ) ) );
    }

    @Test
    public void metadataStoreShouldCreateInputFolderIfNotExist() throws IOException
    {
        final var folder = testDirectory.directory( "folder1" );
        fs.delete( folder );
        final var elements = List.of( "a", "b" );

        //when
        metadataStore.write( folder, elements );

        //then
        assertEquals( List.of("a;","b;"), readLines( MetadataStore.getFilePath( folder ) ) );
    }

    @Test
    public void metadataStoreShouldBeAbleToIdentifyMetadataStoreFile() throws IOException
    {
        final var folder = testDirectory.directory( "folder1" );
        final var elements = List.of( "a", "b" );

        //when
        metadataStore.write( folder, elements );

        //then
        assertTrue( MetadataStore.isMetadataFile( MetadataStore.getFilePath( folder ) ) );
        assertFalse( MetadataStore.isMetadataFile( Path.of( "test" ) ) );
    }

    private List<String> readLines( Path path ) throws IOException
    {
        try ( Stream<String> stream = Files.lines( path ) )
        {
            return stream.collect( Collectors.toList() );
        }
    }
}
