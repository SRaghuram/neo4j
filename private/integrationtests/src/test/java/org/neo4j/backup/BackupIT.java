/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.backup.impl.BackupExecutionException;
import org.neo4j.backup.impl.ConsistencyCheckExecutionException;
import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.common.DependencyResolver;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.DaemonThreadFactory;

import static com.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyArray;
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
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

@PageCacheExtension
@ExtendWith( {RandomExtension.class, SuppressOutputExtension.class} )
class BackupIT
{
    private static final String LABEL = "Cat";
    private static final String PROPERTY = "name";

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

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );

        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @TestWithRecordFormats
    void makeSureFullWorksWhenNoDb( String recordFormatName ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        DbRepresentation initialDataSet = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackupWithoutFallbackToFull( db );

        assertEquals( initialDataSet, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void backedUpDatabaseContainsChecksumOfLastTx( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackup( db );
        db.shutdown();

        long firstChecksum = lastTxChecksumOf( serverStoreLayout, pageCache );
        assertNotEquals( 0, firstChecksum );
        assertEquals( firstChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );

        addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackupWithoutFallbackToFull( db );
        db.shutdown();

        long secondChecksum = lastTxChecksumOf( serverStoreLayout, pageCache );
        assertNotEquals( 0, secondChecksum );
        assertEquals( secondChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
        assertNotEquals( firstChecksum, secondChecksum );
    }

    @TestWithRecordFormats
    void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertEquals( 0, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
    }

    @TestWithRecordFormats
    void shouldFindTransactionLogContainingLastNeoStoreTransaction( String recordFormatName ) throws Exception
    {

        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        createInitialDataSet( db );
        createIndex( db );
        createNode( db );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertNotEquals( 0, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
    }

    @TestWithRecordFormats
    void fullThenIncremental( String recordFormatName ) throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        executeBackup( db );

        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        db.shutdown();

        DbRepresentation furtherRepresentation = addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackupWithoutFallbackToFull( db );

        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void makeSureNoLogFileRemains( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        // First check full
        executeBackup( db );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );

        // Then check empty incremental
        executeBackupWithoutFallbackToFull( db );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );

        // Then check real incremental
        db.shutdown();
        addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );

        executeBackupWithoutFallbackToFull( db );
        assertFalse( checkLogFileExistence( backupDatabasePath.getPath() ) );
    }

    @TestWithRecordFormats
    void makeSureStoreIdIsEnforced( String recordFormatName ) throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverStorePath, recordFormatName );
        GraphDatabaseService db = startDb( serverStorePath );

        // Grab initial backup from server A
        executeBackup( db );
        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        db.shutdown();

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath, recordFormatName );
        addMoreData( otherServerPath, recordFormatName );
        db = startDb( otherServerPath );

        // Try to grab incremental backup from server B.
        // Data should be OK, but store id check should prevent that.
        final GraphDatabaseService finalDb = db;
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( finalDb ) );
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
        db.shutdown();

        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverStorePath, recordFormatName );
        db = startDb( serverStorePath );
        executeBackupWithoutFallbackToFull( db );
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

        executeBackup( db );
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
            executeBackupWithoutFallbackToFull( db );
            assertEquals( lastCommittedTx + i + 1, getLastCommittedTx( backupDatabaseLayout, pageCache ) );
        }
    }

    @TestWithRecordFormats
    void backupMultipleSchemaIndexes( String recordFormatName ) throws Exception
    {
        // given
        ExecutorService executor = newSingleThreadedExecutor();
        AtomicBoolean end = new AtomicBoolean();
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        int numberOfIndexedLabels = 10;
        List<Label> indexedLabels = createIndexes( db, numberOfIndexedLabels );

        // start thread that continuously writes to indexes
        executor.submit( () ->
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
        executor.shutdown();

        // create backup
        executeBackup( db );

        end.set( true );
        assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
    }

    @TestWithRecordFormats
    void shouldBackupEmptyStore( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void shouldRetainFileLocksAfterFullBackupOnLiveDatabase( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        assertStoreIsLocked( serverStorePath );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertStoreIsLocked( serverStorePath );
    }

    @TestWithRecordFormats
    void shouldIncrementallyBackupDenseNodes( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        createInitialDataSet( db );

        executeBackup( db );
        DbRepresentation representation = addLotsOfData( db );

        executeBackupWithoutFallbackToFull( db );
        assertEquals( representation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void shouldLeaveIdFilesAfterBackup( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, recordFormatName );
        createInitialDataSet( db );

        executeBackup( db );
        ensureStoresHaveIdFiles( backupDatabaseLayout );

        DbRepresentation representation = addLotsOfData( db );

        executeBackupWithoutFallbackToFull( db );
        assertEquals( representation, getBackupDbRepresentation() );
        ensureStoresHaveIdFiles( backupDatabaseLayout );
    }

    @TestWithRecordFormats
    void backupDatabaseWithCustomTransactionLogsLocation( String recordFormatName ) throws Exception
    {
        String customTxLogsLocation = testDirectory.directory( "customLogLocation" ).getAbsolutePath();
        Map<Setting<?>,String> settings = Maps.fixedSize.of( record_format, recordFormatName, transaction_logs_root_path, customTxLogsLocation );
        GraphDatabaseService db = startDb( serverStorePath, settings );
        createInitialDataSet( db );

        LogFiles backupLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDatabasePath, fs ).build();

        executeBackup( db );
        assertThat( backupLogFiles.logFiles(), arrayWithSize( 1 ) );

        DbRepresentation representation = addLotsOfData( db );
        executeBackupWithoutFallbackToFull( db );
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

            ExecutorService executor = newSingleThreadedExecutor();
            Future<Object> serverAcceptFuture = executor.submit( () ->
            {
                // accept a connection and immediately close it
                Socket socket = serverSocket.accept();
                socket.close();
                return null;
            } );
            executor.shutdown();

            BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackup( "localhost", port ) );
            assertThat( rootCause( error ), either( instanceOf( ConnectException.class ) ).or( instanceOf( ClosedChannelException.class ) ) );

            assertNull( serverAcceptFuture.get( 1, TimeUnit.MINUTES ) );
            assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
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

        executeBackup( db );

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

        executeBackup( db );

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

        executeBackup( db );

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
        executeBackup( db );

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
        executeBackupWithoutFallbackToFull( db );

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
        executeBackupWithoutFallbackToFull( db );

        // it should be possible to at this point to start db based on our backup and create couple of properties
        // their ids should not clash with already existing
        GraphDatabaseService backupDb = startDbWithoutOnlineBackup( backupDatabasePath );
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
        GraphDatabaseService db = startDb( serverStorePath );
        createIndex( db );

        for ( int i = 0; i < 100; i++ )
        {
            createNode( db );
        }

        File oldLog = dependencyResolver( db ).resolveDependency( LogFiles.class ).getHighestLogFile();
        rotateAndCheckPoint( db );

        for ( int i = 0; i < 1; i++ )
        {
            createNode( db );
        }
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = getLastCommittedTx( db );

        db.shutdown();
        FileUtils.deleteFile( oldLog );
        GraphDatabaseService dbAfterRestart = startDb( serverStorePath );

        long lastCommittedTxAfter = getLastCommittedTx( dbAfterRestart );

        // when
        assertDoesNotThrow( () -> executeBackup( dbAfterRestart ) );

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
        flushAndForce( db );

        // when
        long lastCommittedTx = getLastCommittedTx( db );
        executeBackup( db );
        db.shutdown();

        // then
        // pull transactions is always executed after a full backup
        // it starts from the transaction before the one received last
        long expectedPreviousCommittedTx = lastCommittedTx - 1;
        checkPreviousCommittedTxIdFromBackupTxLog( 0, expectedPreviousCommittedTx );
    }

    @Test
    void shouldContainTransactionsThatHappenDuringBackupProcessWhenBackupEmptyStore() throws Exception
    {
        testTransactionsDuringFullBackup( 0 );
    }

    @Test
    void shouldContainTransactionsThatHappenDuringBackupProcessWhenBackupNonEmptyStore() throws Exception
    {
        testTransactionsDuringFullBackup( random.nextInt( 50, 2000 ) );
    }

    @Test
    void shouldPerformConsistencyCheckAfterBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );
        corruptStore( db );

        ConsistencyCheckExecutionException error = assertThrows( ConsistencyCheckExecutionException.class, () -> executeBackup( db ) );

        assertThat( error.getMessage(), containsString( "Inconsistencies found" ) );
        String[] reportFiles = findBackupInconsistenciesReports();
        assertThat( reportFiles, arrayWithSize( 1 ) );
    }

    @Test
    void shouldNotPerformConsistencyCheckAfterBackupWhenDisabled() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );
        corruptStore( db );

        OnlineBackupContext context = defaultBackupContextBuilder( backupAddress( db ) )
                .withConsistencyCheck( false ) // no consistency check after backup
                .build();

        // backup does not fail
        assertDoesNotThrow( () -> executeBackup( context ) );

        // no consistency check report files
        String[] reportFiles = findBackupInconsistenciesReports();
        assertThat( reportFiles, emptyArray() );

        // store is inconsistent after backup
        ConsistencyCheckService.Result backupConsistencyCheckResult = checkConsistency( backupDatabaseLayout );
        assertFalse( backupConsistencyCheckResult.isSuccessful() );
    }

    @Test
    void shouldFailIncrementalBackupWhenLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        GraphDatabaseService db = prepareDatabaseWithTooOldBackup();

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );
        Throwable cause = error.getCause();
        assertThat( cause, instanceOf( StoreCopyFailedException.class ) );
        assertThat( cause.getMessage(), containsString( "Pulling tx failed consecutively without progress" ) );
    }

    @Test
    void shouldFallbackToFullBackupWhenLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        GraphDatabaseService db = prepareDatabaseWithTooOldBackup();

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldCleanupUnusableBackupsAfterSuccessfulBackupOfTheSameStore() throws Exception
    {
        int staleBackupsCount = 5;
        GraphDatabaseService db = prepareDatabaseWithTooOldBackup();

        // trigger a number of failed incremental backups that should fallback to full
        for ( int i = 0; i < staleBackupsCount; i++ )
        {
            forceTransactionLogRotation( db );
            executeBackup( db );
        }

        File[] dirs = backupsDir.listFiles();
        assertNotNull( dirs );
        assertEquals( singletonList( new File( backupsDir, DEFAULT_DATABASE_NAME ) ), Arrays.asList( dirs ) );
    }

    @Test
    void shouldWorkWithReadOnlyDatabases() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );
        addLotsOfData( db );
        db.shutdown();

        db = startDb( serverStorePath, singletonMap( read_only, TRUE ) );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldThrowWhenExistingBackupIsFromSeparatelyUpgradedStore() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath );
        addLotsOfData( db );

        executeBackup( db );
        setUpgradeTimeInMetaDataStore( backupDatabaseLayout, pageCache, 424242 );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );
        assertThat( error.getCause(), instanceOf( StoreIdDownloadFailedException.class ) );
    }

    @Test
    void shouldNotServeTransactionsWithInvalidHighIds() throws Exception
    {
        /*
         * This is in effect a high level test for an edge case that happens when a relationship group is
         * created and deleted in the same tx.
         *
         * The way we try to trigger this is:
         * 0. In one tx, create a node with 49 relationships, belonging to two types.
         * 1. In another tx, create another relationship on that node (making it dense) and then delete all
         *    relationships of one type. This results in the tx state having a relationship group record that was
         *    created in this tx and also set to not in use.
         * 2. Receipt of this tx will have the offending rel group command apply its id before the groups that are
         *    altered. This will try to update the high id with a value larger than what has been seen previously and
         *    fail the update.
         * The situation is resolved by a check added in TransactionRecordState which skips the creation of such
         * commands.
         */
        GraphDatabaseService db = startDb( serverStorePath );
        createInitialDataSet( db );

        executeBackup( db );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );

        createTransactionWithWeirdRelationshipGroupRecord( db );

        executeBackupWithoutFallbackToFull( db );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    private void createTransactionWithWeirdRelationshipGroupRecord( GraphDatabaseService db )
    {
        Node node;
        RelationshipType typeToDelete = RelationshipType.withName( "A" );
        RelationshipType theOtherType = RelationshipType.withName( "B" );
        int defaultDenseNodeThreshold = Integer.parseInt( dense_node_threshold.getDefaultValue() );

        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < defaultDenseNodeThreshold - 1; i++ )
            {
                node.createRelationshipTo( db.createNode(), theOtherType );
            }
            node.createRelationshipTo( db.createNode(), typeToDelete );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.createRelationshipTo( db.createNode(), theOtherType );
            for ( Relationship relationship : node.getRelationships( Direction.BOTH, typeToDelete ) )
            {
                relationship.delete();
            }
            tx.success();
        }
    }

    private GraphDatabaseService prepareDatabaseWithTooOldBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverStorePath, singletonMap( keep_logical_logs, "false" ) );

        createInitialDataSet( db );
        createIndex( db );
        createNode( db );
        rotateAndCheckPoint( db );

        executeBackup( db ); // full backup should be successful

        // commit multiple transactions and rotate transaction logs to make incremental backup not possible
        forceTransactionLogRotation( db );

        return db;
    }

    private void forceTransactionLogRotation( GraphDatabaseService db ) throws IOException
    {
        for ( int i = 0; i < 1000; i++ )
        {
            createNode( db );
            rotateAndCheckPoint( db );
        }
    }

    private static void corruptStore( GraphDatabaseService db ) throws Exception
    {
        List<StorageCommand> commands = new ArrayList<>();

        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                long id = node.getId();

                NodeRecord before = new NodeRecord( id );
                before.initialize( true, NO_NEXT_PROPERTY.intValue(), false, NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );

                NodeRecord after = new NodeRecord( id );
                after.initialize( true, 42, true, 42, 42 );

                commands.add( new Command.NodeCommand( before, after ) );
            }
        }

        PhysicalTransactionRepresentation txRepresentation = new PhysicalTransactionRepresentation( commands );
        txRepresentation.setHeader( new byte[0], 42, 42, 42, 42, 42, 42 );
        TransactionToApply txToApply = new TransactionToApply( txRepresentation );

        TransactionCommitProcess commitProcess = dependencyResolver( db ).resolveDependency( TransactionCommitProcess.class );
        commitProcess.commit( txToApply, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
    }

    private static ConsistencyCheckService.Result checkConsistency( DatabaseLayout layout ) throws ConsistencyCheckIncompleteException
    {
        Config config = Config.builder()
                .withSetting( pagecache_memory, "8m" )
                .build();

        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();

        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( System.out );
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );

        return consistencyCheckService.runFullConsistencyCheck( layout, config, progressMonitorFactory, logProvider, true, consistencyFlags );
    }

    private void testTransactionsDuringFullBackup( int nodesInDbBeforeBackup ) throws Exception
    {
        int transactionsDuringBackup = 10;//random.nextInt( 10, 1000 );

        GraphDatabaseService db = startDb( serverStorePath );
        createIndexAndNodes( db, nodesInDbBeforeBackup );
        long lastCommittedTxIdBeforeBackup = getLastCommittedTx( db );

        Barrier.Control barrier = new Barrier.Control();
        Monitors monitors = dependencyResolver( db ).resolveDependency( Monitors.class );
        monitors.addMonitorListener( new BackupClientPausingMonitor( barrier, serverStoreLayout ) );

        ExecutorService executor = newSingleThreadedExecutor();
        Future<Void> midBackupTransactionsFuture = executor.submit( () ->
        {
            barrier.awaitUninterruptibly();

            for ( int i = 0; i < transactionsDuringBackup; i++ )
            {
                createNode( db );
            }

            flushAndForce( db );
            barrier.release();
            return null;
        } );

        executeBackup( db, monitors );

        executor.shutdown();
        assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
        assertNull( midBackupTransactionsFuture.get() );

        long lastCommittedTxIdAfterBackup = getLastCommittedTx( db );
        long lastCommittedTxIdFromBackup = getLastCommittedTx( backupDatabaseLayout, pageCache );
        long labelAndPropertyTokenTransactions = nodesInDbBeforeBackup == 0 ? 2 : 0;
        long expectedLastCommittedTxIdAfterBackup = lastCommittedTxIdBeforeBackup + transactionsDuringBackup + labelAndPropertyTokenTransactions;

        assertEquals( expectedLastCommittedTxIdAfterBackup, lastCommittedTxIdAfterBackup );
        assertEquals( lastCommittedTxIdAfterBackup, lastCommittedTxIdFromBackup );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
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
                "Could build up database in same process, store not locked" );

        assertThat( error.getCause().getCause(), instanceOf( StoreLockException.class ) );
    }

    private DbRepresentation addMoreData( File path, String recordFormatName )
    {
        GraphDatabaseService db = startDbWithoutOnlineBackup( path, recordFormatName );
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
        GraphDatabaseService db = startDbWithoutOnlineBackup( path, recordFormatName );
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
        return startDb( path, emptyMap() );
    }

    private GraphDatabaseService startDb( File path, String recordFormatName )
    {
        return startDb( path, singletonMap( record_format, recordFormatName ) );
    }

    private GraphDatabaseService startDbWithoutOnlineBackup( File path )
    {
        return startDbWithoutOnlineBackup( path, record_format.getDefaultValue() );
    }

    private GraphDatabaseService startDbWithoutOnlineBackup( File path, String recordFormatName )
    {
        Map<Setting<?>,String> settings = Maps.fixedSize.of( online_backup_enabled, FALSE, record_format, recordFormatName );
        return startDb( path, settings );
    }

    private GraphDatabaseService startDb( File path, Map<Setting<?>,String> settings )
    {
        GraphDatabaseBuilder builder = new TestCommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( path );

        for ( Map.Entry<Setting<?>,String> entry : settings.entrySet() )
        {
            builder.setConfig( entry.getKey(), entry.getValue() );
        }

        GraphDatabaseService db = builder.newGraphDatabase();
        databases.add( db );
        return db;
    }

    private DbRepresentation getBackupDbRepresentation()
    {
        Config config = Config.builder()
                .withSetting( online_backup_enabled, FALSE )
                .withSetting( GraphDatabaseSettings.active_database, backupDatabasePath.getName() ).build();
        return DbRepresentation.of( backupDatabasePath, config );
    }

    private void executeBackup( GraphDatabaseService db ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        executeBackup( backupAddress( db ), new Monitors(), true );
    }

    private void executeBackupWithoutFallbackToFull( GraphDatabaseService db ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        executeBackup( backupAddress( db ), new Monitors(), false );
    }

    private void executeBackup( GraphDatabaseService db, Monitors monitors ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        executeBackup( backupAddress( db ), monitors, true );
    }

    private void executeBackup( String hostname, int port ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        executeBackup( new AdvertisedSocketAddress( hostname, port ), new Monitors(), true );
    }

    private void executeBackup( AdvertisedSocketAddress address, Monitors monitors, boolean fallbackToFull )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        OnlineBackupContext context = defaultBackupContextBuilder( address )
                .withFallbackToFullBackup( fallbackToFull )
                .build();

        executeBackup( context, monitors );
    }

    private void executeBackup( OnlineBackupContext context ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        executeBackup( context, new Monitors() );
    }

    private void executeBackup( OnlineBackupContext context, Monitors monitors ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                .withOutputStream( System.out )
                .withLogProvider( FormattedLogProvider.toOutputStream( System.out ) )
                .withMonitors( monitors )
                .build();

        executor.executeBackup( context );
    }

    private OnlineBackupContext.Builder defaultBackupContextBuilder( AdvertisedSocketAddress address )
    {
        Path dir = backupDatabasePath.getParentFile().toPath();

        return OnlineBackupContext.builder()
                .withAddress( address )
                .withBackupDirectory( dir )
                .withReportsDirectory( dir );
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

    private void createIndexAndNodes( GraphDatabaseService db, int count )
    {
        if ( count > 0 )
        {
            createIndex( db );
            for ( int i = 0; i < count; i++ )
            {
                createNode( db );
            }
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

    private static void setUpgradeTimeInMetaDataStore( DatabaseLayout databaseLayout, PageCache pageCache, long value ) throws IOException
    {
        MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(), Position.UPGRADE_TIME, value );
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

    private static void createIndex( GraphDatabaseService db )
    {
        createIndex( db, LABEL, PROPERTY );
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

    private void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( LABEL ) ).setProperty( PROPERTY, random.nextString() );
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

    private String[] findBackupInconsistenciesReports()
    {
        return backupsDir.list( ( dir, name ) -> name.contains( "inconsistencies" ) && name.contains( "report" ) );
    }

    private static void rotateAndCheckPoint( GraphDatabaseService db ) throws IOException
    {
        DependencyResolver resolver = dependencyResolver( db );
        resolver.resolveDependency( LogRotation.class ).rotateLogFile();
        resolver.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private void flushAndForce( GraphDatabaseService db ) throws IOException
    {
        DependencyResolver resolver = dependencyResolver( db );
        StorageEngine storageEngine = resolver.resolveDependency( StorageEngine.class );
        storageEngine.flushAndForce( IOLimiter.UNLIMITED );
    }

    private static AdvertisedSocketAddress backupAddress( GraphDatabaseService db )
    {
        DependencyResolver resolver = dependencyResolver( db );
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( BACKUP_SERVER_NAME );
        assertNotNull( address, "Backup server address not registered" );
        return new AdvertisedSocketAddress( address.getHost(), address.getPort() );
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static ExecutorService newSingleThreadedExecutor()
    {
        return Executors.newSingleThreadExecutor( new DaemonThreadFactory() );
    }

    private static class BackupClientPausingMonitor extends StoreCopyClientMonitor.Adapter
    {
        final Barrier barrier;
        final DatabaseLayout databaseLayout;

        BackupClientPausingMonitor( Barrier barrier, DatabaseLayout databaseLayout )
        {
            this.barrier = barrier;
            this.databaseLayout = databaseLayout;
        }

        @Override
        public void finishReceivingStoreFile( String file )
        {
            if ( file.endsWith( databaseLayout.nodeStore().getName() ) || file.endsWith( databaseLayout.relationshipStore().getName() ) )
            {
                barrier.reached(); // multiple calls to this barrier will not block
            }
        }
    }
}
