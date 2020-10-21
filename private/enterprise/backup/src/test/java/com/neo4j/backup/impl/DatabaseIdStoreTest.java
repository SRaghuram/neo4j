/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.impl.DatabaseIdStore.CHARSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class DatabaseIdStoreTest
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private DatabaseIdStore databaseIdStore;
    private Path databaseDir;

    @BeforeEach
    void setUp()
    {
        databaseDir = testDirectory.directory( "myDb" );
        LogProvider logProvider = Mockito.mock( LogProvider.class );
        Log log = Mockito.mock( Log.class );
        when( logProvider.getLog( DatabaseIdStore.class ) ).thenReturn( log );
        databaseIdStore = new DatabaseIdStore( fs, logProvider );
    }

    @Test
    void shouldWriteEndReadDatabaseId() throws IOException
    {
        final var expected = DatabaseIdFactory.from( UUID.randomUUID() );

        //when
        databaseIdStore.writeDatabaseId( expected, databaseDir );

        //then
        assertThat( Optional.of( expected ) ).isEqualTo( databaseIdStore.readDatabaseId( databaseDir ) );
    }

    @Test
    void shouldOverwriteValueInCaseOfTwoSequentWrite() throws IOException
    {
        final var expected = DatabaseIdFactory.from( UUID.randomUUID() );

        //when
        databaseIdStore.writeDatabaseId( DatabaseIdFactory.from( UUID.randomUUID() ), databaseDir );
        databaseIdStore.writeDatabaseId( expected, databaseDir );

        //then
        assertThat( Optional.of( expected ) ).isEqualTo( databaseIdStore.readDatabaseId( databaseDir ) );
    }

    @Test
    void shouldCreateInputFolderIfNotExist() throws IOException
    {
        final var expected = DatabaseIdFactory.from( UUID.randomUUID() );
        fs.delete( databaseDir );

        //when
        databaseIdStore.writeDatabaseId( expected, databaseDir );

        //then
        assertThat( Optional.of( expected ) ).isEqualTo( databaseIdStore.readDatabaseId( databaseDir ) );
    }

    @Test
    void shouldReadEmptyDatabaseIdIfFileDoesntExist()
    {
        assertThat( databaseIdStore.readDatabaseId( databaseDir ) ).isEmpty();
    }

    @Test
    void shouldReadEmptyDatabaseIdIfFileIsCorrupted() throws IOException
    {
        writeToFile( DatabaseIdStore.getDatabaseFilePath( databaseDir ), "File is corrupted" );

        assertThat( databaseIdStore.readDatabaseId( databaseDir ) ).isEmpty();
    }

    private void writeToFile( Path filePath, String value ) throws IOException
    {
        final var fs = new DefaultFileSystemAbstraction();
        try ( StoreChannel channel = fs.write( filePath ) )
        {
            channel.writeAll( ByteBuffer.wrap( value.getBytes( CHARSET ) ) );
        }
    }
}
