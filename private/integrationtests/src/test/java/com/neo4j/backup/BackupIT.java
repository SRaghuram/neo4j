/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.backup.impl.BackupExecutionException;
import org.neo4j.backup.impl.ConsistencyCheckExecutionException;
import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.scheduler.DaemonThreadFactory;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Integer.parseInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

@PageCacheExtension
@ExtendWith( {RandomExtension.class, SuppressOutputExtension.class} )
class BackupIT
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private RandomRule random;

    private File serverStorePath;
    private File otherServerPath;
    private File backupDatabasePath;
    private File backupsDir;
    private List<GraphDatabaseService> databases;
    private DatabaseLayout backupDatabaseLayout;
    private DatabaseLayout serverStoreLayout;

    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @ParameterizedTest( name = "{0}" )
    @ValueSource( strings = {Standard.LATEST_NAME, HighLimit.NAME} )
    @interface TestWithRecordFormats
    {
    }

    @BeforeEach
    void beforeEach()
    {
        databases = new ArrayList<>();
        serverStoreLayout = DatabaseLayout.of( testDirectory.storeDir( "server" ), DEFAULT_DATABASE_NAME );
        serverStorePath = serverStoreLayout.databaseDirectory();
        otherServerPath = DatabaseLayout.of( testDirectory.storeDir( "otherServer" ), DEFAULT_DATABASE_NAME ).databaseDirectory();
        backupsDir = testDirectory.storeDir( "backups" );
        backupDatabaseLayout = DatabaseLayout.of( backupsDir, DEFAULT_DATABASE_NAME );
        backupDatabasePath = backupDatabaseLayout.databaseDirectory();
    }

    @AfterEach
    void afterEach()
    {
        databases.forEach( GraphDatabaseService::shutdown );
        databases.clear();
    }

    @TestWithRecordFormats
    void makeSureFullFailsWhenDifferentDbExists( String recordFormatName )
    {
        createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( backupDatabasePath, recordFormatName );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackup( db, false ) );

        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @TestWithRecordFormats
    void makeSureFullWorksWhenNoDb( String recordFormatName ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        DbRepresentation initialDataSet = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackup( db, false );

        assertEquals( initialDataSet, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void backedUpDatabaseContainsChecksumOfLastTx( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackup( db, true );
        db.shutdown();

        long firstChecksum = lastTxChecksumOf( serverStoreLayout, pageCache );
        assertNotEquals( 0, firstChecksum );
        assertEquals( firstChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );

        addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackup( db, false );
        db.shutdown();

        long secondChecksum = lastTxChecksumOf( serverStoreLayout, pageCache );
        assertNotEquals( 0, secondChecksum );
        assertEquals( secondChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
        assertNotEquals( firstChecksum, secondChecksum );
    }

    @TestWithRecordFormats
    @Disabled( "Backup of an empty store in single instance does not work" )
    void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore( String recordFormatName ) throws Exception
    {
        // This test highlights a special case where an empty store can return transaction metadata for transaction 0.

        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );

        executeBackup( db, true );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertEquals( 0, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
    }

    @TestWithRecordFormats
    void fullThenIncremental( String recordFormatName ) throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackup( db, true );

        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        db.shutdown();

        DbRepresentation furtherRepresentation = addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackup( db, false );

        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void makeSureNoLogFileRemains( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        // First check full
        executeBackup( db, true );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );

        // Then check empty incremental
        executeBackup( db, false );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );

        // Then check real incremental
        db.shutdown();
        addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackup( db, false );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );
    }

    @TestWithRecordFormats
    void makeSureStoreIdIsEnforced( String recordFormatName ) throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        // Grab initial backup from server A
        executeBackup( db, true );
        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        db.shutdown();

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath, recordFormatName );
        addMoreData( otherServerPath, recordFormatName );
        db = startDb( otherServerPath );

        // Try to grab incremental backup from server B.
        // Data should be OK, but store id check should prevent that.
        final GraphDatabaseService finalDb = db;
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackup( finalDb, false ) );
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
        db.shutdown();

        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );
        executeBackup( db, false );
        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void multipleIncrementals( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( Label.label( "Label" ) );
            node.setProperty( "Key", "Value" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "TYPE" ) );
            tx.success();
        }

        executeBackup( db, true );
        long lastCommittedTx = getLastCommittedTx( backupDatabaseLayout, pageCache );

        for ( int i = 0; i < 5; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( Label.label( "Label" ) );
                node.setProperty( "Key", "Value" );
                db.createNode().createRelationshipTo( node, RelationshipType.withName( "TYPE" ) );
                tx.success();
            }
            executeBackup( db, false );
            assertEquals( lastCommittedTx + i + 1, getLastCommittedTx( backupDatabaseLayout, pageCache ) );
        }
    }

    @TestWithRecordFormats
    void backupMultipleSchemaIndexes( String recordFormatName ) throws Exception
    {
        // given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        AtomicBoolean end = new AtomicBoolean();
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        int numberOfIndexedLabels = 10;
        List<Label> indexedLabels = createIndexes( db, numberOfIndexedLabels );

        // start thread that continuously writes to indexes
        executorService.submit( () ->
        {
            while ( !end.get() )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode( indexedLabels.get( random.nextInt( numberOfIndexedLabels ) ) ).setProperty( "prop", random.nextValue() );
                    tx.success();
                }
            }
        } );
        executorService.shutdown();

        // create backup
        executeBackup( db, true );

        end.set( true );
        executorService.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @TestWithRecordFormats
    @Disabled( "Backup of an empty store in single instance does not work" )
    void shouldBackupEmptyStore( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );

        executeBackup( db, true );
    }

    @TestWithRecordFormats
    @Disabled( "Backup of an empty store in single instance does not work" )
    void shouldRetainFileLocksAfterFullBackupOnLiveDatabase( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        assertStoreIsLocked( serverStorePath );

        executeBackup( db, true );
        assertStoreIsLocked( serverStorePath );
    }

    @TestWithRecordFormats
    void shouldIncrementallyBackupDenseNodes( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        createInitialDataSet( db );

        executeBackup( db, true );
        DbRepresentation representation = addLotsOfData( db );

        executeBackup( db, false );
        assertEquals( representation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void shouldLeaveIdFilesAfterBackup( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        createInitialDataSet( db );

        executeBackup( db, true );
        ensureStoresHaveIdFiles( backupDatabaseLayout );

        DbRepresentation representation = addLotsOfData( db );

        executeBackup( db, false );
        assertEquals( representation, getBackupDbRepresentation() );
        ensureStoresHaveIdFiles( backupDatabaseLayout );
    }

    @TestWithRecordFormats
    void backupDatabaseWithCustomTransactionLogsLocation( String recordFormatName ) throws Exception
    {
        String customTxLogsLocation = testDirectory.directory( "customLogLocation" ).getAbsolutePath();
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName, true, customTxLogsLocation );
        createInitialDataSet( db );

        LogFiles backupLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDatabasePath, fs ).build();

        executeBackup( db, true );
        assertThat( backupLogFiles.logFiles(), arrayWithSize( 1 ) );

        DbRepresentation representation = addLotsOfData( db );
        executeBackup( db, false );
        assertThat( backupLogFiles.logFiles(), arrayWithSize( 1 ) );

        assertEquals( representation, getBackupDbRepresentation() );

        long lastCommittedTxInDb = getLastCommittedTx( db );
        long lastCommittedTxInBackup = getLastCommittedTx( backupDatabaseLayout, pageCache );
        assertEquals( lastCommittedTxInDb, lastCommittedTxInBackup );
    }

    @Test
    void shouldThrowUsefulExceptionWhenUnableToConnect() throws Exception
    {
        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            int port = serverSocket.getLocalPort();

            ExecutorService executor = Executors.newSingleThreadExecutor( new DaemonThreadFactory() );
            Future<Object> serverAcceptFuture = executor.submit( () ->
            {
                // accept a connection and immediately close it
                Socket socket = serverSocket.accept();
                socket.close();
                return null;
            } );
            executor.shutdown();

            BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackup( "localhost", port, true ) );
            assertThat( rootCause( error ), either( instanceOf( ConnectException.class ) ).or( instanceOf( ClosedChannelException.class ) ) );

            assertNull( serverAcceptFuture.get( 1, TimeUnit.MINUTES ) );
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldCopyInvalidFileFromBackupDirectoryToErrorDirectoryAndDoFullBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );

        assertTrue( backupDatabaseLayout.databaseDirectory().mkdirs() );
        File incorrectFile = backupDatabaseLayout.file( ".jibberishfile" );
        fs.create( incorrectFile ).close();

        executeBackup( db, true );

        // unexpected file was moved to an error directory
        File incorrectExistingBackupDir = new File( backupsDir, "graph.db.err.0" );
        assertTrue( fs.isDirectory( incorrectExistingBackupDir ) );
        assertTrue( fs.fileExists( new File( incorrectExistingBackupDir, incorrectFile.getName() ) ) );

        // no temporary directories are present, i.e. 'graph.db.temp.0'
        assertThat( backupsDir.list(), arrayContainingInAnyOrder( DEFAULT_DATABASE_NAME, "graph.db.err.0" ) );

        // backup produced a correct database
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldCopyInvalidDirectoryFromBackupDirectoryToErrorDirectoryAndDoFullBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );

        File incorrectDir = backupDatabaseLayout.file( "jibberishfolder" );
        File incorrectFile = new File( incorrectDir, "jibberishfile" );
        fs.mkdirs( incorrectDir );
        fs.create( incorrectFile ).close();

        executeBackup( db, true );

        // unexpected directory was moved to an error directory
        File incorrectExistingBackupDir = new File( backupsDir, "graph.db.err.0" );
        assertTrue( fs.isDirectory( incorrectExistingBackupDir ) );
        File movedIncorrectDir = new File( incorrectExistingBackupDir, incorrectDir.getName() );
        assertTrue( fs.isDirectory( movedIncorrectDir ) );
        assertTrue( fs.fileExists( new File( movedIncorrectDir, incorrectFile.getName() ) ) );

        // no temporary directories are present, i.e. 'graph.db.temp.0'
        assertThat( backupsDir.list(), arrayContainingInAnyOrder( DEFAULT_DATABASE_NAME, "graph.db.err.0" ) );

        // backup produced a correct database
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldCopyStoreFiles() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );
        addLotsOfData( db );
        createIndexes( db, 42 );

        executeBackup( db, true );

        File[] backupStoreFiles = backupDatabaseLayout.databaseDirectory().listFiles();
        assertNotNull( backupStoreFiles );
        assertThat( backupStoreFiles, arrayWithSize( greaterThan( 0 ) ) );

        for ( File storeFile : backupDatabaseLayout.storeFiles() )
        {
            if ( backupDatabaseLayout.countStoreA().equals( storeFile ) || backupDatabaseLayout.countStoreB().equals( storeFile ) )
            {
                assertThat( backupStoreFiles, hasItemInArray(
                        either( equalTo( backupDatabaseLayout.countStoreA() ) ).or( equalTo( backupDatabaseLayout.countStoreB() ) ) ) );
            }
            else
            {
                assertThat( backupStoreFiles, hasItemInArray( storeFile ) );
            }
        }

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    /*
     * During incremental backup destination db should not track free ids independently from source db
     * for now we will always cleanup id files generated after incremental backup and will regenerate them afterwards
     * This should prevent situation when destination db free id following the source db, but never allocates it from
     * generator till some db will be started on top of it.
     * That will cause all sorts of problems with several entities in a store with same id.
     *
     * As soon as backup will be able to align ids between participants please remove description and adapt test.
     */
    @Test
    void incrementallyBackupDatabaseShouldNotKeepGeneratedIdFiles() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        Label markerLabel = Label.label( "marker" );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode();
            node.addLabel( markerLabel );
            transaction.success();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( db, markerLabel );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "property" + i, "testValue" + i );
            }
            transaction.success();
        }
        // propagate to backup node and properties
        executeBackup( db, true );

        // removing properties will free couple of ids that will be reused during next properties creation
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( db, markerLabel );
            for ( int i = 0; i < 6; i++ )
            {
                node.removeProperty( "property" + i );
            }

            transaction.success();
        }

        // propagate removed properties
        executeBackup( db, false );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( db, markerLabel );
            for ( int i = 10; i < 16; i++ )
            {
                node.setProperty( "property" + i, "updatedValue" + i );
            }

            transaction.success();
        }

        // propagate to backup new properties with reclaimed ids
        executeBackup( db, false );

        // it should be possible to at this point to start db based on our backup and create couple of properties
        // their ids should not clash with already existing
        GraphDatabaseService backupDb = startDb( backupDatabasePath, null, false );
        try
        {
            try ( Transaction transaction = backupDb.beginTx() )
            {
                Node node = findNodeByLabel( backupDb, markerLabel );
                Iterable<String> propertyKeys = node.getPropertyKeys();
                for ( String propertyKey : propertyKeys )
                {
                    node.setProperty( propertyKey, "updatedClientValue" + propertyKey );
                }
                node.setProperty( "newProperty", "updatedClientValue" );
                transaction.success();
            }

            try ( Transaction ignored = backupDb.beginTx() )
            {
                Node node = findNodeByLabel( backupDb, markerLabel );
                // newProperty + 10 defined properties.
                assertEquals( 11, Iterables.count( node.getPropertyKeys() ), "We should be able to see all previously defined properties." );
            }
        }
        finally
        {
            backupDb.shutdown();
        }
    }

    @Test
    void shouldBeAbleToBackupEvenIfTransactionLogsAreIncomplete() throws Exception
    {
        /*
         * This test deletes the old persisted log file and expects backup to still be functional. It
         * should not be assumed that the log files have any particular length of history. They could
         * for example have been mangled during backups or removed during pruning.
         */

        // given
        String label = "Node";
        String property = "property";

        GraphDatabaseService db = startDb( serverStorePath );
        createIndex( db, label, property );

        for ( int i = 0; i < 100; i++ )
        {
            createNode( db, label, property );
        }

        File oldLog = dependencyResolver( db ).resolveDependency( LogFiles.class ).getHighestLogFile();
        rotateAndCheckPoint( db );

        for ( int i = 0; i < 1; i++ )
        {
            createNode( db, label, property );
        }
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = getLastCommittedTx( db );

        db.shutdown();
        FileUtils.deleteFile( oldLog );
        GraphDatabaseService dbAfterRestart = startDb( serverStorePath );

        long lastCommittedTxAfter = getLastCommittedTx( dbAfterRestart );

        // when
        assertDoesNotThrow( () -> executeBackup( dbAfterRestart, true ) );

        // then
        assertEquals( lastCommittedTxBefore, lastCommittedTxAfter );
        assertEquals( DbRepresentation.of( dbAfterRestart ), getBackupDbRepresentation() );
    }

    @Test
    void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Exception
    {
        // given
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );
        dependencyResolver( db ).resolveDependency( StorageEngine.class ).flushAndForce( IOLimiter.UNLIMITED );

        // when
        long lastCommittedTx = getLastCommittedTx( db );
        executeBackup( db, true );
        db.shutdown();

        // then
        // pull transactions is always executed after a full backup
        // it starts from the transaction before the one received last
        long expectedPreviousCommittedTx = lastCommittedTx - 1;
        checkPreviousCommittedTxIdFromBackupTxLog( 0, expectedPreviousCommittedTx );
    }

    private void ensureStoresHaveIdFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        for ( File idFile : databaseLayout.idFiles() )
        {
            assertTrue( idFile.exists(), "Missing id file " + idFile );
            assertTrue( IdGeneratorImpl.readHighId( fs, idFile ) > 0, "Id file " + idFile + " had 0 highId" );
        }
    }

    private void assertStoreIsLocked( File path )
    {
        RuntimeException error = assertThrows( RuntimeException.class, () -> startDb( path ),
                "Could start up database in same process, store not locked" );

        assertThat( error.getCause().getCause(), instanceOf( StoreLockException.class ) );
    }

    private DbRepresentation addMoreData( File path, String recordFormatName )
    {
        GraphDatabaseService db = startDb( path, recordFormatName, false );
        DbRepresentation representation;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "backup", "Is great" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "LOVES" ) );
            tx.success();
        }
        finally
        {
            representation = DbRepresentation.of( db );
            db.shutdown();
        }
        return representation;
    }

    private DbRepresentation createInitialDataSet( File path, String recordFormatName )
    {
        GraphDatabaseService db = startDb( path, recordFormatName, false );
        try
        {
            createInitialDataSet( db );
            return DbRepresentation.of( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService startDb( File path )
    {
        return startDb( path, null );
    }

    private GraphDatabaseService startDb( File path, String recordFormatName )
    {
        return startDb( path, recordFormatName, true );
    }

    private GraphDatabaseService startDb( File path, String recordFormatName, boolean onlineBackupEnabled )
    {
        return startDb( path, recordFormatName, onlineBackupEnabled, null );
    }

    private GraphDatabaseService startDb( File path, String recordFormatName, boolean onlineBackupEnabled, String txLogsLocation )
    {
        GraphDatabaseBuilder builder = new TestCommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.toString( onlineBackupEnabled ) );

        if ( recordFormatName != null )
        {
            builder.setConfig( GraphDatabaseSettings.record_format, recordFormatName );
        }
        if ( txLogsLocation != null )
        {
            builder.setConfig( GraphDatabaseSettings.transaction_logs_root_path, txLogsLocation );
        }

        GraphDatabaseService db = builder.newGraphDatabase();
        databases.add( db );
        return db;
    }

    private DbRepresentation getBackupDbRepresentation()
    {
        Config config = Config.builder()
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withSetting( GraphDatabaseSettings.active_database, backupDatabasePath.getName() ).build();
        return DbRepresentation.of( backupDatabasePath, config );
    }

    private void executeBackup( GraphDatabaseService db, boolean fallbackToFull ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        DependencyResolver resolver = dependencyResolver( db );
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort backupServerAddress = portRegister.getLocalAddress( BACKUP_SERVER_NAME );
        assertNotNull( backupServerAddress, "Backup server address not registered" );

        executeBackup( backupServerAddress.getHost(), backupServerAddress.getPort(), fallbackToFull );
    }

    private void executeBackup( String hostname, int port, boolean fallbackToFull ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        Path dir = backupDatabasePath.getParentFile().toPath();

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( hostname, port )
                .withBackupDirectory( dir )
                .withReportsDirectory( dir )
                .withFallbackToFullBackup( fallbackToFull )
                .build();

        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                .withOutputStream( System.out )
                .withLogProvider( FormattedLogProvider.toOutputStream( System.out ) )
                .build();

        executor.executeBackup( context );
    }

    private static DbRepresentation addLotsOfData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            int threshold = parseInt( dense_node_threshold.getDefaultValue() );
            for ( int i = 0; i < threshold * 2; i++ )
            {
                node.createRelationshipTo( db.createNode(), TEST );
            }
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    private static void createInitialDataSet( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( Label.label( "Me" ) );
            node.setProperty( "myKey", "myValue" );
            db.createNode( Label.label( "NotMe" ) ).createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
    }

    private static Node findNodeByLabel( GraphDatabaseService db, Label label )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label ) )
        {
            return nodes.next();
        }
    }

    private static boolean checkLogFileExistence( String directory )
    {
        return Config.defaults( logs_directory, directory ).get( store_internal_log_path ).exists();
    }

    private static long lastTxChecksumOf( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        File neoStore = databaseLayout.metadataStore();
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
    }

    private static long getLastCommittedTx( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        File neoStore = databaseLayout.metadataStore();
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
    }

    private static long getLastCommittedTx( GraphDatabaseService db )
    {
        return dependencyResolver( db ).resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private static List<Label> createIndexes( GraphDatabaseService db, int indexCount )
    {
        List<Label> indexedLabels = new ArrayList<>( indexCount );
        for ( int i = 0; i < indexCount; i++ )
        {
            Label label = Label.label( "label" + i );
            indexedLabels.add( label );
            createIndex( db, label.name(), "prop" );
        }
        return indexedLabels;
    }

    private static void createIndex( GraphDatabaseService db, String labelName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( labelName ) ).on( propertyName ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private void createNode( GraphDatabaseService db, String labelName, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( labelName ) ).setProperty( propertyName, random.nextString() );
            tx.success();
        }
    }

    private void checkPreviousCommittedTxIdFromBackupTxLog( long logVersion, long txId ) throws IOException
    {
        // Assert header of specified log version containing correct txId
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDatabaseLayout.databaseDirectory(), fs ).build();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fs, logFiles.getLogFileForVersion( logVersion ) );
        assertEquals( txId, logHeader.lastCommittedTxId );
    }

    private static void rotateAndCheckPoint( GraphDatabaseService db ) throws IOException
    {
        DependencyResolver resolver = dependencyResolver( db );
        resolver.resolveDependency( LogRotation.class ).rotateLogFile();
        resolver.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }
}
