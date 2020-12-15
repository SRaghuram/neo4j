/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

@PageCacheExtension
class StoreInfoCommandEnterpriseTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private Path databaseDirectory;
    private StoreInfoCommand command;
    private PrintStream out;

    @BeforeEach
    void setUp() throws Exception
    {
        Path homeDir = testDirectory.directory( "home-dir" );
        databaseDirectory = homeDir.resolve( "data/databases/foo" );
        Files.createDirectories( databaseDirectory );
        out = mock( PrintStream.class );
        command = new StoreInfoCommand( new ExecutionContext( homeDir, homeDir.resolve( "conf" ), out, mock( PrintStream.class ),
                testDirectory.getFileSystem() ) );
    }

    @Test
    void readsEnterpriseStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( HighLimitV3_4_0.RECORD_FORMATS.storeVersion() );
        CommandLine.populateCommand( command, databaseDirectory.toAbsolutePath().toString() );

        command.execute();

        verify( out ).println( "Database name:                foo" + System.lineSeparator() +
                             "Database in use:              false" + System.lineSeparator() +
                             "Store format version:         vE.H.4" + System.lineSeparator() +
                             "Store format introduced in:   3.4.0" + System.lineSeparator() +
                             "Store format superseded in:   4.0.0" + System.lineSeparator() +
                             "Last committed transaction id:-1" + System.lineSeparator() +
                             "Store needs recovery:         true" );
        verifyNoMoreInteractions( out );
    }

    private void prepareNeoStoreFile( String storeVersion ) throws IOException
    {
        Path neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value, NULL );
    }

    private Path createNeoStoreFile() throws IOException
    {
        fileSystem.mkdir( databaseDirectory );
        Path neoStoreFile = DatabaseLayout.ofFlat( databaseDirectory ).metadataStore();
        fileSystem.write( neoStoreFile ).close();
        return neoStoreFile;
    }
}
