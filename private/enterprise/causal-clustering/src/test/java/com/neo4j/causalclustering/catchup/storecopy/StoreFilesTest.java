/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
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

    private Path databaseDir;
    private DatabaseLayout databaseLayout;
    private Path otherDatabaseDir;
    private DatabaseLayout otherDatabaseLayout;
    private LogFiles logFiles;
    private LogFiles otherLogFiles;

    @BeforeEach
    void beforeEach() throws Exception
    {
        databaseDir = testDirectory.directoryPath( "databasedir" );
        databaseLayout = DatabaseLayout.ofFlat( databaseDir );
        otherDatabaseDir = testDirectory.directoryPath( "otherdatabasedir" );
        otherDatabaseLayout = DatabaseLayout.ofFlat( otherDatabaseDir );
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDir, fs ).build();
        otherLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( otherDatabaseDir, fs ).build();
    }

    @Test
    void shouldDeleteFilesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        List<Path> files = Arrays.asList(
                createFile( databaseDir, "KnownFile1" ),
                createFile( databaseDir, "KnownFile2" ),
                createFile( databaseDir, "KnownFile3" ) );

        storeFiles.delete( databaseLayout, logFiles );

        for ( Path file : files )
        {
            assertFalse( fs.fileExists( file.toFile() ) );
        }
    }

    @Test
    void shouldDeleteDirectoriesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "knownDirectory" ) );

        List<Path> directories = Arrays.asList(
                createDirectory( databaseDir, "knownDirectory1" ),
                createDirectory( databaseDir, "knownDirectory2" ),
                createDirectory( databaseDir, "knownDirectory3" ) );

        for ( Path directory : directories )
        {
            createFile( directory, "dummy-file" );
        }

        storeFiles.delete( databaseLayout, logFiles );

        for ( Path directory : directories )
        {
            assertFalse( fs.fileExists( directory.toFile() ) );
        }
    }

    @Test
    void shouldDeleteTransactionLogs() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();
        var logFile = logFiles.getLogFile();
        Path[] txLogFiles = {logFile.getLogFileForVersion( 1 ), logFile.getLogFileForVersion( 2 ), logFile.getLogFileForVersion( 42 )};
        for ( Path txLogFile : txLogFiles )
        {
            createFile( txLogFile );
        }

        storeFiles.delete( databaseLayout, logFiles );

        for ( Path txLogFile : txLogFiles )
        {
            assertFalse( fs.fileExists( txLogFile.toFile() ) );
        }
    }

    @Test
    void shouldNotDeleteFilesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        Path file1 = createFile( databaseDir, "UnknownFile1" );
        Path file2 = createFile( databaseDir, "KnownFile2" );
        Path file3 = createFile( databaseDir, "UnknownFile3" );

        storeFiles.delete( databaseLayout, logFiles );

        assertTrue( fs.fileExists( file1.toFile() ) );
        assertFalse( fs.fileExists( file2.toFile() ) );
        assertTrue( fs.fileExists( file3.toFile() ) );
    }

    @Test
    void shouldNotDeleteDirectoriesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        Path dir1 = createDirectory( databaseDir, "UnknownDirectory1" );
        Path dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        Path dir3 = createDirectory( databaseDir, "UnknownDirectory3" );

        storeFiles.delete( databaseLayout, logFiles );

        assertTrue( fs.isDirectory( dir1.toFile() ) );
        assertFalse( fs.isDirectory( dir2.toFile() ) );
        assertFalse( fs.fileExists( dir2.toFile() ) );
        assertTrue( fs.isDirectory( dir3.toFile() ) );
    }

    @Test
    void shouldMoveFilesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        Path file1 = createFile( databaseDir, "KnownFile1" );
        Path file2 = createFile( databaseDir, "KnownFile2" );
        Path file3 = createFile( databaseDir, "KnownFile3" );

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        assertFalse( fs.fileExists( file1.toFile() ) );
        assertFalse( fs.fileExists( file2.toFile() ) );
        assertFalse( fs.fileExists( file3.toFile() ) );

        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownFile1" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownFile2" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownFile3" ).toFile() ) );
    }

    @Test
    void shouldMoveDirectoriesThatMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        Path dir1 = createDirectory( databaseDir, "KnownDirectory1" );
        Path dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        Path dir3 = createDirectory( databaseDir, "KnownDirectory3" );

        createFile( dir1, "dummy-file-1" );
        createFile( dir2, "dummy-file-2" );
        createFile( dir3, "dummy-file-3" );

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        assertFalse( fs.fileExists( dir1.toFile() ) );
        assertFalse( fs.fileExists( dir2.toFile() ) );
        assertFalse( fs.fileExists( dir3.toFile() ) );

        assertTrue( fs.isDirectory( otherDatabaseDir.resolve( "KnownDirectory1" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownDirectory1" ).resolve( "dummy-file-1" ).toFile() ) );

        assertTrue( fs.isDirectory( otherDatabaseDir.resolve( "KnownDirectory2" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownDirectory2" ).resolve( "dummy-file-2" ).toFile() ) );

        assertTrue( fs.isDirectory( otherDatabaseDir.resolve( "KnownDirectory3" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownDirectory3" ).resolve( "dummy-file-3" ).toFile() ) );
    }

    @Test
    void shouldMoveTransactionLogs() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();
        LogFile logFile = logFiles.getLogFile();
        Path[] txLogFiles = {logFile.getLogFileForVersion( 99 ), logFile.getLogFileForVersion( 100 ), logFile.getLogFileForVersion( 101 )};
        for ( Path txLogFile : txLogFiles )
        {
            createFile( txLogFile );
        }

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        for ( Path txLogFile : txLogFiles )
        {
            assertFalse( fs.fileExists( txLogFile.toFile() ) );
            Path copiedTxLogFile = otherDatabaseDir.resolve( txLogFile.getFileName().toString() );
            assertTrue( fs.fileExists( copiedTxLogFile.toFile() ) );
        }
    }

    @Test
    void shouldNotMoveFilesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownFile" ) );

        Path file1 = createFile( databaseDir, "UnknownFile1" );
        Path file2 = createFile( databaseDir, "KnownFile2" );
        Path file3 = createFile( databaseDir, "UnknownFile3" );

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        assertTrue( fs.fileExists( file1.toFile() ) );
        assertFalse( fs.fileExists( file2.toFile() ) );
        assertTrue( fs.fileExists( file3.toFile() ) );

        assertFalse( fs.fileExists( otherDatabaseDir.resolve( "UnknownFile1" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownFile2" ).toFile() ) );
        assertFalse( fs.fileExists( otherDatabaseDir.resolve( "UnknownFile3" ).toFile() ) );
    }

    @Test
    void shouldNotMoveDirectoriesThatDoNotMatchTheFilter() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles( name -> name.startsWith( "KnownDirectory" ) );

        Path dir1 = createDirectory( databaseDir, "UnknownDirectory1" );
        Path dir2 = createDirectory( databaseDir, "KnownDirectory2" );
        Path dir3 = createDirectory( databaseDir, "UnknownDirectory3" );

        Path file1 = createFile( dir1, "dummy-file-1" );
        Path file2 = createFile( dir2, "dummy-file-2" );
        Path file3 = createFile( dir3, "dummy-file-3" );

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        assertTrue( fs.isDirectory( dir1.toFile() ) );
        assertTrue( fs.fileExists( file1.toFile() ) );
        assertFalse( fs.isDirectory( dir2.toFile() ) );
        assertFalse( fs.fileExists( file2.toFile() ) );
        assertTrue( fs.isDirectory( dir3.toFile() ) );
        assertTrue( fs.fileExists( file3.toFile() ) );

        assertFalse( fs.isDirectory( otherDatabaseDir.resolve( "UnknownDirectory1" ).toFile() ) );
        assertFalse( fs.fileExists( otherDatabaseDir.resolve( "UnknownDirectory1" ).resolve( "dummy-file-1" ).toFile() ) );
        assertTrue( fs.isDirectory( otherDatabaseDir.resolve( "KnownDirectory2" ).toFile() ) );
        assertTrue( fs.fileExists( otherDatabaseDir.resolve( "KnownDirectory2" ).resolve( "dummy-file-2" ).toFile() ) );
        assertFalse( fs.isDirectory( otherDatabaseDir.resolve( "UnknownDirectory3" ).toFile() ) );
        assertFalse( fs.fileExists( otherDatabaseDir.resolve( "UnknownDirectory3" ).resolve( "dummy-file-3" ).toFile() ) );
    }

    @Test
    void shouldCheckIfNonExistingDirectoryIsEmpty()
    {
        StoreFiles storeFiles = newStoreFiles();

        Path nonExistingDirectory = Path.of( "Non/ExistingDirectory" );
        DatabaseLayout layout = DatabaseLayout.ofFlat( nonExistingDirectory );

        assertTrue( storeFiles.isEmpty( layout ) );
    }

    @Test
    void shouldCheckIfEmptyDirectoryIsEmpty()
    {
        StoreFiles storeFiles = newStoreFiles();

        Path emptyDirectory = testDirectory.directoryPath( "EmptyDirectory" );
        DatabaseLayout layout = DatabaseLayout.ofFlat( emptyDirectory );

        assertTrue( storeFiles.isEmpty( layout ) );
    }

    @Test
    void shouldCheckDirectoryWithDatabaseFilesIsEmpty() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        createFile( databaseDir, DatabaseFile.METADATA_STORE.getName() );
        createFile( databaseDir, DatabaseFile.NODE_STORE.getName() );
        createFile( databaseDir, DatabaseFile.RELATIONSHIP_STORE.getName() );

        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDir );

        assertFalse( storeFiles.isEmpty( databaseLayout ) );
    }

    @Test
    void shouldReadStoreIdWhenMetadataStoreExists() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();
        Path metadataStore = databaseDir.resolve( DatabaseFile.METADATA_STORE.getName() );
        createFile( metadataStore );

        ThreadLocalRandom random = ThreadLocalRandom.current();
        long creationTime = random.nextLong();
        long randomId = random.nextLong();
        long storeVersion = random.nextLong();
        long upgradeTime = random.nextLong();
        long upgradeId = random.nextLong();

        MetaDataStore.setRecord( pageCache, metadataStore, TIME, creationTime, NULL );
        MetaDataStore.setRecord( pageCache, metadataStore, RANDOM_NUMBER, randomId, NULL );
        MetaDataStore.setRecord( pageCache, metadataStore, STORE_VERSION, storeVersion, NULL );
        MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TIME, upgradeTime, NULL );
        MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID, upgradeId, NULL );

        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDir );

        StoreId storeId = storeFiles.readStoreId( databaseLayout, NULL );

        assertEquals( new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId ), storeId );
    }

    @Test
    void shouldFailToReadStoreIdWhenMetadataIsMissing()
    {
        StoreFiles storeFiles = newStoreFiles();

        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDir );

        assertThrows( IOException.class, () -> storeFiles.readStoreId( databaseLayout, NULL ) );
    }

    @Test
    void shouldNotDeleteTempCopyDirectory() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        Path tempCopyDir = createDirectory( databaseDir, "temp-copy" );
        Path notTempCopyDir = createDirectory( databaseDir, "not-temp-copy" );
        assertTrue( fs.isDirectory( tempCopyDir.toFile() ) );
        assertTrue( fs.isDirectory( notTempCopyDir.toFile() ) );

        storeFiles.delete( databaseLayout, logFiles );

        assertTrue( fs.isDirectory( tempCopyDir.toFile() ) );
        assertFalse( fs.isDirectory( notTempCopyDir.toFile() ) );
    }

    @Test
    void shouldNotMoveTempCopyDirectory() throws Exception
    {
        StoreFiles storeFiles = newStoreFiles();

        Path tempCopyDir = createDirectory( databaseDir, "temp-copy" );
        Path notTempCopyDir = createDirectory( databaseDir, "not-temp-copy" );
        assertTrue( fs.isDirectory( tempCopyDir.toFile() ) );
        assertTrue( fs.isDirectory( notTempCopyDir.toFile() ) );

        storeFiles.moveTo( databaseDir.toFile(), otherDatabaseLayout, otherLogFiles );

        assertTrue( fs.isDirectory( tempCopyDir.toFile() ) );
        assertFalse( fs.isDirectory( otherDatabaseDir.resolve( "temp-copy" ).toFile() ) );
        assertFalse( fs.isDirectory( notTempCopyDir.toFile() ) );
        assertTrue( fs.isDirectory( otherDatabaseDir.resolve( "not-temp-copy" ).toFile() ) );
    }

    private Path createFile( Path parentDir, String name ) throws IOException
    {
        return createFile( parentDir.resolve( name ) );
    }

    private Path createFile( Path file ) throws IOException
    {
        fs.mkdirs( file.getParent().toFile() );
        fs.write( file.toFile() ).close();
        assertTrue( fs.fileExists( file.toFile() ) );
        return file;
    }

    private Path createDirectory( Path parentDir, String name ) throws IOException
    {
        Path dir = parentDir.resolve( name );
        fs.mkdirs( dir.toFile() );
        assertTrue( fs.isDirectory( dir.toFile() ) );
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
