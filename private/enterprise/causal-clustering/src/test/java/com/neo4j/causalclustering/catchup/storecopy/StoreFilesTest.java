/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.identity.StoreId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;

@PageCacheExtension
class StoreFilesTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    private File databaseDir;
    private DatabaseLayout databaseLayout;
    private File otherDatabaseDir;
    private DatabaseLayout otherDatabaseLayout;
    private LogFiles logFiles;
    private LogFiles otherLogFiles;

    @BeforeEach
    void beforeEach() throws Exception
    {
        databaseDir = testDirectory.directory( "databaseDir" );
        databaseLayout = DatabaseLayout.of( databaseDir );
        otherDatabaseDir = testDirectory.directory( "otherDatabaseDir" );
        otherDatabaseLayout = DatabaseLayout.of( otherDatabaseDir );
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDir, fs ).build();
        otherLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( otherDatabaseDir, fs ).build();
    }

    @Test
    void shouldDeleteFilesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        List<File> files = Arrays.asList(
                createFile( databaseDir, "KnownFile1" ),
                createFile( databaseDir, "KnownFile2" ),
                createFile( databaseDir, "KnownFile3" ) );

        storeFiles.delete( databaseLayout, logFiles );

        for ( File file : files )
        {
            assertFalse( fs.fileExists( file ) );
        }
    }

    @Test
    void shouldDeleteDirectoriesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        List<File> directories = Arrays.asList(
                createDirectory( databaseDir, "KnownDirectory1" ),
                createDirectory( databaseDir, "KnownDirectory2" ),
                createDirectory( databaseDir, "KnownDirectory3" ) );

        for ( File directory : directories )
        {
            createFile( directory, "dummy-file" );
        }

        storeFiles.delete( databaseLayout, logFiles );

        for ( File directory : directories )
        {
            assertFalse( fs.fileExists( directory ) );
        }
    }

    @Test
    void shouldDeleteTransactionLogs() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        File[] txLogFiles = {logFiles.getLogFileForVersion( 1 ), logFiles.getLogFileForVersion( 2 ), logFiles.getLogFileForVersion( 42 )};
        for ( File txLogFile : txLogFiles )
        {
            createFile( txLogFile );
        }

        storeFiles.delete( databaseLayout, logFiles );

        for ( File txLogFile : txLogFiles )
        {
            assertFalse( fs.fileExists( txLogFile ) );
        }
    }

    @Test
    void shouldNotDeleteFilesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        File file1 = createFile( databaseDir, "UnknownFile1" );
        File file2 = createFile( databaseDir, "KnownFile2" );
        File file3 = createFile( databaseDir, "UnknownFile3" );

        storeFiles.delete( databaseLayout, logFiles );

        assertTrue( fs.fileExists( file1 ) );
        assertFalse( fs.fileExists( file2 ) );
        assertTrue( fs.fileExists( file3 ) );
    }

    @Test
    void shouldNotDeleteDirectoriesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        File dir1 = createDirectory( databaseDir, "UnknownDirectory1" );
        File dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        File dir3 = createDirectory( databaseDir, "UnknownDirectory3" );

        storeFiles.delete( databaseLayout, logFiles );

        assertTrue( fs.isDirectory( dir1 ) );
        assertFalse( fs.isDirectory( dir2 ) );
        assertFalse( fs.fileExists( dir2 ) );
        assertTrue( fs.isDirectory( dir3 ) );
    }

    @Test
    void shouldMoveFilesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        File file1 = createFile( databaseDir, "KnownFile1" );
        File file2 = createFile( databaseDir, "KnownFile2" );
        File file3 = createFile( databaseDir, "KnownFile3" );

        storeFiles.moveTo( databaseDir, otherDatabaseLayout, otherLogFiles );

        assertFalse( fs.fileExists( file1 ) );
        assertFalse( fs.fileExists( file2 ) );
        assertFalse( fs.fileExists( file3 ) );

        assertTrue( fs.fileExists( new File( otherDatabaseDir, "KnownFile1" ) ) );
        assertTrue( fs.fileExists( new File( otherDatabaseDir, "KnownFile2" ) ) );
        assertTrue( fs.fileExists( new File( otherDatabaseDir, "KnownFile3" ) ) );
    }

    @Test
    void shouldMoveDirectoriesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        File dir1 = createDirectory( databaseDir, "KnownDirectory1" );
        File dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        File dir3 = createDirectory( databaseDir, "KnownDirectory3" );

        createFile( dir1, "dummy-file-1" );
        createFile( dir2, "dummy-file-2" );
        createFile( dir3, "dummy-file-3" );

        storeFiles.moveTo( databaseDir, otherDatabaseLayout, otherLogFiles );

        assertFalse( fs.fileExists( dir1 ) );
        assertFalse( fs.fileExists( dir2 ) );
        assertFalse( fs.fileExists( dir3 ) );

        assertTrue( fs.isDirectory( new File( otherDatabaseDir, "KnownDirectory1" ) ) );
        assertTrue( fs.fileExists( new File( new File( otherDatabaseDir, "KnownDirectory1" ), "dummy-file-1" ) ) );

        assertTrue( fs.isDirectory( new File( otherDatabaseDir, "KnownDirectory2" ) ) );
        assertTrue( fs.fileExists( new File( new File( otherDatabaseDir, "KnownDirectory2" ), "dummy-file-2" ) ) );

        assertTrue( fs.isDirectory( new File( otherDatabaseDir, "KnownDirectory3" ) ) );
        assertTrue( fs.fileExists( new File( new File( otherDatabaseDir, "KnownDirectory3" ), "dummy-file-3" ) ) );
    }

    @Test
    void shouldMoveTransactionLogs() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        File[] txLogFiles = {logFiles.getLogFileForVersion( 99 ), logFiles.getLogFileForVersion( 100 ), logFiles.getLogFileForVersion( 101 )};
        for ( File txLogFile : txLogFiles )
        {
            createFile( txLogFile );
        }

        storeFiles.moveTo( databaseDir, otherDatabaseLayout, otherLogFiles );

        for ( File txLogFile : txLogFiles )
        {
            assertFalse( fs.fileExists( txLogFile ) );
            File copiedTxLogFile = new File( otherDatabaseDir, txLogFile.getName() );
            assertTrue( fs.fileExists( copiedTxLogFile ) );
        }
    }

    @Test
    void shouldNotMoveFilesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        File file1 = createFile( databaseDir, "UnknownFile1" );
        File file2 = createFile( databaseDir, "KnownFile2" );
        File file3 = createFile( databaseDir, "UnknownFile3" );

        storeFiles.moveTo( databaseDir, otherDatabaseLayout, otherLogFiles );

        assertTrue( fs.fileExists( file1 ) );
        assertFalse( fs.fileExists( file2 ) );
        assertTrue( fs.fileExists( file3 ) );

        assertFalse( fs.fileExists( new File( otherDatabaseDir, "UnknownFile1" ) ) );
        assertTrue( fs.fileExists( new File( otherDatabaseDir, "KnownFile2" ) ) );
        assertFalse( fs.fileExists( new File( otherDatabaseDir, "UnknownFile3" ) ) );
    }

    @Test
    void shouldNotMoveDirectoriesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        File dir1 = createDirectory( databaseDir, "UnknownDirectory1" );
        File dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        File dir3 = createDirectory( databaseDir, "UnknownDirectory3" );

        File file1 = createFile( dir1, "dummy-file-1" );
        File file2 = createFile( dir2, "dummy-file-2" );
        File file3 = createFile( dir3, "dummy-file-3" );

        storeFiles.moveTo( databaseDir, otherDatabaseLayout, otherLogFiles );

        assertTrue( fs.isDirectory( dir1 ) );
        assertTrue( fs.fileExists( file1 ) );
        assertFalse( fs.isDirectory( dir2 ) );
        assertFalse( fs.fileExists( file2 ) );
        assertTrue( fs.isDirectory( dir3 ) );
        assertTrue( fs.fileExists( file3 ) );

        assertFalse( fs.isDirectory( new File( otherDatabaseDir, "UnknownDirectory1" ) ) );
        assertFalse( fs.fileExists( new File( new File( otherDatabaseDir, "UnknownDirectory1" ), "dummy-file-1" ) ) );
        assertTrue( fs.isDirectory( new File( otherDatabaseDir, "KnownDirectory2" ) ) );
        assertTrue( fs.fileExists( new File( new File( otherDatabaseDir, "KnownDirectory2" ), "dummy-file-2" ) ) );
        assertFalse( fs.isDirectory( new File( otherDatabaseDir, "UnknownDirectory3" ) ) );
        assertFalse( fs.fileExists( new File( new File( otherDatabaseDir, "UnknownDirectory3" ), "dummy-file-3" ) ) );
    }

    @Test
    void shouldCheckIfNonExistingDirectoryIsEmpty()
    {
        StoreFiles storeFiles = newStoreFiles();

        File nonExistingDirectory = new File( "NonExistingDirectory" );
        DatabaseLayout layout = DatabaseLayout.of( nonExistingDirectory );

        assertTrue( storeFiles.isEmpty( layout ) );
    }

    @Test
    void shouldCheckIfEmptyDirectoryIsEmpty()
    {
        StoreFiles storeFiles = newStoreFiles();

        File emptyDirectory = testDirectory.directory( "EmptyDirectory" );
        DatabaseLayout layout = DatabaseLayout.of( emptyDirectory );

        assertTrue( storeFiles.isEmpty( layout ) );
    }

    @Test
    void shouldCheckDirectoryWithDatabaseFilesIsEmpty() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        createFile( databaseDir, DatabaseFile.METADATA_STORE.getName() );
        createFile( databaseDir, DatabaseFile.NODE_STORE.getName() );
        createFile( databaseDir, DatabaseFile.RELATIONSHIP_STORE.getName() );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDir );

        assertFalse( storeFiles.isEmpty( databaseLayout ) );
    }

    @Test
    void shouldReadStoreIdWhenMetadataStoreExists() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();
        File metadataStore = new File( databaseDir, DatabaseFile.METADATA_STORE.getName() );
        createFile( metadataStore );

        ThreadLocalRandom random = ThreadLocalRandom.current();
        long creationTime = random.nextLong();
        long randomId = random.nextLong();
        long upgradeTime = random.nextLong();
        long upgradeId = random.nextLong();

        MetaDataStore.setRecord( pageCache, metadataStore, TIME, creationTime );
        MetaDataStore.setRecord( pageCache, metadataStore, RANDOM_NUMBER, randomId );
        MetaDataStore.setRecord( pageCache, metadataStore, STORE_VERSION, random.nextLong() );
        MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TIME, upgradeTime );
        MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID, upgradeId );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDir );

        StoreId storeId = storeFiles.readStoreId( databaseLayout );

        assertEquals( new StoreId( creationTime, randomId, upgradeTime, upgradeId ), storeId );
    }

    @Test
    void shouldFailToReadStoreIdWhenMetadataIsMissing()
    {
        StoreFiles storeFiles = newStoreFiles();

        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDir );

        assertThrows( IOException.class, () -> storeFiles.readStoreId( databaseLayout ) );
    }

    @Test
    void shouldNotDeleteTempCopyDirectory()
    {

    }

    @Test
    void shouldNotMoveTempCopyDirectory()
    {

    }

    private File createFile( File parentDir, String name ) throws IOException
    {
        File file = new File( parentDir, name );
        return createFile( file );
    }

    private File createFile( File file ) throws IOException
    {
        fs.mkdirs( file.getParentFile() );
        fs.open( file, OpenMode.READ_WRITE ).close();
        assertTrue( fs.fileExists( file ) );
        return file;
    }

    private File createDirectory( File parentDir, String name ) throws IOException
    {
        File dir = new File( parentDir, name );
        fs.mkdirs( dir );
        assertTrue( fs.isDirectory( dir ) );
        return dir;
    }

    private StoreFiles newStoreFiles( Predicate<String> nameFilter )
    {
        return new StoreFiles( fs, pageCache, ( dir, name ) -> nameFilter.test( name ) );
    }

    private StoreFiles newStoreFiles()
    {
        return new StoreFiles( fs, pageCache );
    }
}
