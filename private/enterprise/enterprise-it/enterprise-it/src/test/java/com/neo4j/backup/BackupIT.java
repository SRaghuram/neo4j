/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.BackupExecutionException;
import com.neo4j.backup.impl.MetadataStore;
import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.backup.impl.local.DatabaseIdStore;
import com.neo4j.backup.impl.tools.ConsistencyCheckExecutionException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.TestWithRecordFormats;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
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
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
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
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.CausalClusteringInternalSettings.experimental_catchup_protocol;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

@PageCacheExtension
@ExtendWith( {RandomExtension.class, SuppressOutputExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
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

    private Path serverHomeDir;
    private Path otherServerPath;
    private Path backupDatabasePath;
    private Path backupsDir;
    private List<GraphDatabaseService> databases;
    private DatabaseLayout backupDatabaseLayout;
    private DatabaseLayout serverDatabaseLayout;
    private DatabaseManagementService managementService;

    @BeforeEach
    void beforeEach()
    {
        databases = new ArrayList<>();
        var serverLayout = Neo4jLayout.ofFlat( testDirectory.homePath( "server" ) );
        serverHomeDir = serverLayout.homeDirectory();
        serverDatabaseLayout = serverLayout.databaseLayout( DEFAULT_DATABASE_NAME );

        var otherServerLayout = Neo4jLayout.ofFlat( testDirectory.homePath( "otherServer" ) );
        otherServerPath = otherServerLayout.databaseLayout( DEFAULT_DATABASE_NAME ).databaseDirectory();

        backupsDir = testDirectory.homePath( "backups" );

        backupDatabaseLayout = DatabaseLayout.ofFlat( backupsDir.resolve( DEFAULT_DATABASE_NAME ) );
        backupDatabasePath = backupDatabaseLayout.databaseDirectory();
    }

    @AfterEach
    void afterEach()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
        databases.clear();
    }

    @TestWithRecordFormats
    void makeSureFullFailsWhenDifferentDbExists( String recordFormatName )
    {
        createInitialDataSet( serverHomeDir, recordFormatName );
        createInitialDataSet( backupsDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );

        assertThat( error.getCause() ).isInstanceOf( StoreIdDownloadFailedException.class );
    }

    @TestWithRecordFormats
    void makeSureFullWorksWhenNoDb( String recordFormatName ) throws Exception
    {
        DbRepresentation initialDataSet = createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        executeBackupWithoutFallbackToFull( db );

        assertEquals( initialDataSet, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void backedUpDatabaseContainsChecksumOfLastTx( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        executeBackup( db );
        managementService.shutdown();

        long firstChecksum = lastTxChecksumOf( serverDatabaseLayout, pageCache );
        assertNotEquals( 0, firstChecksum );
        assertEquals( firstChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );

        addMoreData( serverHomeDir, recordFormatName );
        db = startDb( serverHomeDir );

        executeBackupWithoutFallbackToFull( db );
        managementService.shutdown();

        long secondChecksum = lastTxChecksumOf( serverDatabaseLayout, pageCache );
        assertNotEquals( 0, secondChecksum );
        assertEquals( secondChecksum, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
        assertNotEquals( firstChecksum, secondChecksum );
    }

    @TestWithRecordFormats
    void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertEquals( BASE_TX_CHECKSUM, lastTxChecksumOf( backupDatabaseLayout, pageCache ) );
    }

    @TestWithRecordFormats
    void shouldFindTransactionLogContainingLastNeoStoreTransaction( String recordFormatName ) throws Exception
    {

        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );
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
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        executeBackup( db );

        checkDatabaseIdCorrectness( DEFAULT_DATABASE_NAME );
        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        managementService.shutdown();

        DbRepresentation furtherRepresentation = addMoreData( serverHomeDir, recordFormatName );
        db = startDb( serverHomeDir );

        executeBackupWithoutFallbackToFull( db );

        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
        checkDatabaseIdCorrectness( DEFAULT_DATABASE_NAME );
    }

    @TestWithRecordFormats
    void makeSureNoLogFileRemains( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        // First check full
        executeBackup( db );
        assertFalse( checkLogFileExistence( backupDatabasePath ) );

        // Then check empty incremental
        executeBackupWithoutFallbackToFull( db );
        assertFalse( checkLogFileExistence( backupDatabasePath ) );

        // Then check real incremental
        managementService.shutdown();
        addMoreData( serverHomeDir, recordFormatName );
        db = startDb( serverHomeDir );

        executeBackupWithoutFallbackToFull( db );
        assertFalse( checkLogFileExistence( backupDatabasePath ) );
    }

    @TestWithRecordFormats
    void makeSureStoreIdIsEnforced( String recordFormatName ) throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        // Grab initial backup from server A
        executeBackup( db );
        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        managementService.shutdown();

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath, recordFormatName );
        addMoreData( otherServerPath, recordFormatName );
        db = startDb( otherServerPath );

        // Try to grab incremental backup from server B.
        // Data should be OK, but store id check should prevent that.
        final GraphDatabaseService finalDb = db;
        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( finalDb ) );
        assertThat( error.getCause() ).isInstanceOf( StoreIdDownloadFailedException.class );
        managementService.shutdown();

        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverHomeDir, recordFormatName );
        db = startDb( serverHomeDir );
        executeBackupWithoutFallbackToFull( db );
        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void multipleIncrementals( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( Label.label( "Label" ) );
            node.setProperty( "Key", "Value" );
            tx.createNode().createRelationshipTo( node, RelationshipType.withName( "TYPE" ) );
            tx.commit();
        }

        executeBackup( db );
        long lastCommittedTx = getLastCommittedTx( backupDatabaseLayout, pageCache );

        for ( int i = 0; i < 5; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( Label.label( "Label" ) );
                node.setProperty( "Key", "Value" );
                tx.createNode().createRelationshipTo( node, RelationshipType.withName( "TYPE" ) );
                tx.commit();
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
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );
        int numberOfIndexedLabels = 10;
        List<Label> indexedLabels = createIndexes( db, numberOfIndexedLabels );

        // start thread that continuously writes to indexes
        executor.submit( () ->
                         {
                             while ( !end.get() )
                             {
                                 try ( Transaction tx = db.beginTx() )
                                 {
                                     tx.createNode( indexedLabels.get( random.nextInt( numberOfIndexedLabels ) ) )
                                       .setProperty( "prop", random.nextValueAsObject() );
                                     tx.commit();
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
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void shouldRetainFileLocksAfterFullBackupOnLiveDatabase( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );
        assertStoreIsLocked( serverHomeDir );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertStoreIsLocked( serverHomeDir );
    }

    @TestWithRecordFormats
    void shouldIncrementallyBackupDenseNodes( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );
        createInitialDataSet( db );

        executeBackup( db );
        DbRepresentation representation = addLotsOfData( db );

        executeBackupWithoutFallbackToFull( db );
        assertEquals( representation, getBackupDbRepresentation() );
    }

    @TestWithRecordFormats
    void shouldLeaveIdFilesAfterBackup( String recordFormatName ) throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, recordFormatName );
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
        Path customTxLogsLocation = testDirectory.directory( "customLogLocation" ).toAbsolutePath();
        Map<Setting<?>,Object> settings = Maps.mutable.of( record_format, recordFormatName, transaction_logs_root_path, customTxLogsLocation );
        GraphDatabaseService db = startDb( serverHomeDir, settings );
        createInitialDataSet( db );

        StorageEngineFactory storageEngineFactory = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        LogFiles backupLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDatabasePath, fs )
                                                 .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                                                 .build();

        executeBackup( db );
        assertThat( backupLogFiles.logFiles() ).hasSize( 2 );

        DbRepresentation representation = addLotsOfData( db );
        executeBackupWithoutFallbackToFull( db );
        assertThat( backupLogFiles.logFiles() ).hasSize( 2 );

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
            assertThat( getRootCause( error ) ).isInstanceOfAny( ConnectException.class, ClosedChannelException.class );

            assertNull( serverAcceptFuture.get( 1, TimeUnit.MINUTES ) );
            assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
        }
    }

    @Test
    void shouldCopyInvalidFileFromBackupDirectoryToErrorDirectoryAndDoFullBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );

        Files.createDirectories( backupDatabaseLayout.databaseDirectory() );
        Path incorrectFile = backupDatabaseLayout.file( ".jibberishfile" );
        fs.write( incorrectFile ).close();

        executeBackup( db );

        // unexpected file was moved to an error directory
        Path incorrectExistingBackupDir = backupsDir.resolve( "neo4j.err.0" );
        assertTrue( fs.isDirectory( incorrectExistingBackupDir ) );
        assertTrue( fs.fileExists( incorrectExistingBackupDir.resolve( incorrectFile.getFileName() ) ) );

        // no temporary directories are present, i.e. 'neo4j.temp.0'
        try ( Stream<Path> list = Files.list( backupsDir ) )
        {
            assertThat( list.map( Path::getFileName ).map( Path::toString ).collect( Collectors.toList() ) )
                    .contains( DEFAULT_DATABASE_NAME, "neo4j.err.0" );
        }

        // backup produced a correct database
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldCopyInvalidDirectoryFromBackupDirectoryToErrorDirectoryAndDoFullBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );

        Path incorrectDir = backupDatabaseLayout.file( "jibberishfolder" );
        Path incorrectFile = incorrectDir.resolve( "jibberishfile" );
        fs.mkdirs( incorrectDir );
        fs.write( incorrectFile ).close();

        executeBackup( db );

        // unexpected directory was moved to an error directory
        Path incorrectExistingBackupDir = backupsDir.resolve( "neo4j.err.0" );
        assertTrue( fs.isDirectory( incorrectExistingBackupDir ) );
        Path movedIncorrectDir = incorrectExistingBackupDir.resolve( incorrectDir.getFileName() );
        assertTrue( fs.isDirectory( movedIncorrectDir ) );
        assertTrue( fs.fileExists( movedIncorrectDir.resolve( incorrectFile.getFileName() ) ) );

        // no temporary directories are present, i.e. 'neo4j.temp.0'
        try ( Stream<Path> list = Files.list( backupsDir ) )
        {
            assertThat( list.map( Path::getFileName ).map( Path::toString ).collect( Collectors.toList() ) )
                    .contains( DEFAULT_DATABASE_NAME, "neo4j.err.0" );
        }

        // backup produced a correct database
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldCopyStoreFiles() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );
        addLotsOfData( db );
        createIndexes( db, 42 );

        executeBackup( db );

        Path[] backupStoreFiles = fs.listFiles( backupDatabaseLayout.databaseDirectory() );
        assertThat( backupStoreFiles ).hasSizeGreaterThan( 0 );

        for ( Path storeFile : backupDatabaseLayout.storeFiles() )
        {
            if ( backupDatabaseLayout.countStore().equals( storeFile ) )
            {
                assertThat( backupStoreFiles ).contains( backupDatabaseLayout.countStore() );
            }
            else
            {
                if ( DatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName().equals( storeFile.getFileName().toString() ) &&
                     !Config.defaults().get( enable_relationship_type_scan_store ) )
                {
                    // Skip relationship type scan store file if feature is not enabled
                    continue;
                }
                assertThat( backupStoreFiles ).contains( storeFile );
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
        GraphDatabaseService db = startDb( serverHomeDir );
        Label markerLabel = Label.label( "marker" );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = transaction.createNode();
            node.addLabel( markerLabel );
            transaction.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( transaction, markerLabel );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "property" + i, "testValue" + i );
            }
            transaction.commit();
        }
        // propagate to backup node and properties
        executeBackup( db );

        // removing properties will free couple of ids that will be reused during next properties creation
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( transaction, markerLabel );
            for ( int i = 0; i < 6; i++ )
            {
                node.removeProperty( "property" + i );
            }

            transaction.commit();
        }

        // propagate removed properties
        executeBackupWithoutFallbackToFull( db );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = findNodeByLabel( transaction, markerLabel );
            for ( int i = 10; i < 16; i++ )
            {
                node.setProperty( "property" + i, "updatedValue" + i );
            }

            transaction.commit();
        }

        // propagate to backup new properties with reclaimed ids
        executeBackupWithoutFallbackToFull( db );
        managementService.shutdown();

        // it should be possible to at this point to start db based on our backup and create couple of properties
        // their ids should not clash with already existing
        GraphDatabaseService backupDb = startDbWithoutOnlineBackup( backupsDir );
        try
        {
            try ( Transaction transaction = backupDb.beginTx() )
            {
                Node node = findNodeByLabel( transaction, markerLabel );
                Iterable<String> propertyKeys = node.getPropertyKeys();
                for ( String propertyKey : propertyKeys )
                {
                    node.setProperty( propertyKey, "updatedClientValue" + propertyKey );
                }
                node.setProperty( "newProperty", "updatedClientValue" );
                transaction.commit();
            }

            try ( Transaction transaction = backupDb.beginTx() )
            {
                Node node = findNodeByLabel( transaction, markerLabel );
                // newProperty + 10 defined properties.
                assertEquals( 11, Iterables.count( node.getPropertyKeys() ), "We should be able to see all previously defined properties." );
            }
        }
        finally
        {
            managementService.shutdown();
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
        GraphDatabaseService db = startDb( serverHomeDir );
        createIndex( db );

        for ( int i = 0; i < 100; i++ )
        {
            createNode( db );
        }

        Path oldLog = dependencyResolver( db ).resolveDependency( LogFiles.class ).getLogFile().getHighestLogFile();
        rotateAndCheckPoint( db );

        for ( int i = 0; i < 1; i++ )
        {
            createNode( db );
        }
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = getLastCommittedTx( db );

        managementService.shutdown();
        FileUtils.deleteFile( oldLog );
        GraphDatabaseService dbAfterRestart = startDb( serverHomeDir );

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
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );
        flushAndForce( db );

        // when
        long lastCommittedTx = getLastCommittedTx( db );
        executeBackup( db );
        managementService.shutdown();

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
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );
        corruptStore( db );

        ConsistencyCheckExecutionException error = assertThrows( ConsistencyCheckExecutionException.class, () -> executeBackup( db ) );

        assertThat( error.getMessage() ).contains( "Inconsistencies found" );
        String[] reportFiles = findBackupInconsistenciesReports();
        assertThat( reportFiles ).hasSize( 1 );
    }

    @Test
    void shouldNotPerformConsistencyCheckAfterBackupWhenDisabled() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );
        corruptStore( db );

        var contextBuilder = defaultBackupContextBuilder( backupAddress( db ) )
                .withConsistencyCheck( false ); // no consistency check after backup

        // backup does not fail
        assertDoesNotThrow( () -> executeBackup( contextBuilder.build() ) );

        // no consistency check report files
        String[] reportFiles = findBackupInconsistenciesReports();
        assertThat( reportFiles ).isEmpty();

        // store is inconsistent after backup
        ConsistencyCheckService.Result backupConsistencyCheckResult = checkConsistency( backupDatabaseLayout ); // wrong file location
        assertFalse( backupConsistencyCheckResult.isSuccessful() );
    }

    @Test
    void shouldFailIncrementalBackupWhenLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        GraphDatabaseService db = prepareDatabaseWithTooOldBackup();

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );
        Throwable cause = error.getCause();
        assertThat( cause ).isInstanceOf( StoreCopyFailedException.class );
        assertThat( cause.getMessage() ).contains( "Pulling tx failed consecutively without progress" );
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

        Path[] dirs;
        try ( Stream<Path> list = Files.list( backupsDir ) )
        {
            dirs = list.toArray( Path[]::new );
        }
        assertNotNull( dirs );
        assertEquals( singletonList( backupsDir.resolve( DEFAULT_DATABASE_NAME ) ), Arrays.asList( dirs ) );
    }

    @Test
    void shouldWorkWithReadOnlyDatabases() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );
        addLotsOfData( db );
        managementService.shutdown();

        db = startDb( serverHomeDir, Maps.mutable.of( read_only_database_default, true ) );

        executeBackup( db );

        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldThrowWhenExistingBackupIsFromSeparatelyUpgradedStore() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir );
        addLotsOfData( db );

        executeBackup( db );
        setUpgradeTimeInMetaDataStore( backupDatabaseLayout, pageCache, 424242 );

        BackupExecutionException error = assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( db ) );
        assertThat( error.getCause() ).isInstanceOf( StoreIdDownloadFailedException.class );
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
        GraphDatabaseService db = startDb( serverHomeDir );
        createInitialDataSet( db );

        executeBackup( db );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );

        createTransactionWithWeirdRelationshipGroupRecord( db );

        executeBackupWithoutFallbackToFull( db );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
    }

    @Test
    void shouldFailToBackupUnknownDatabase()
    {
        var unknownDbName = "unknowndb";

        var db = startDb( serverHomeDir );
        createInitialDataSet( db );

        var contextBuilder = defaultBackupContextBuilder( backupAddress( db ) )
                .withDatabaseNamePattern( unknownDbName );

        var error = assertThrows( BackupExecutionException.class, () -> executeBackup( contextBuilder.build() ) );
        assertThat( error.getMessage() ).contains( "Failed to download database id for database '" + unknownDbName  + "'");
    }

    @TestWithRecordFormats
    void shouldBackupMultipleDatabasesIfPatternMatch( String recordFormatName ) throws Exception
    {
        //given
        var defaultDBRepresentation = createInitialDataSet( serverHomeDir, recordFormatName );
        var defaultDB = startDb( serverHomeDir );

        var natureDB = "nature";
        createDatabase( natureDB );
        createInitialDataSet( managementService.database( natureDB ) );

        //given log providers
        final var userLogProvider = new AssertableLogProvider();
        final var internalLogProvider = new AssertableLogProvider();

        //when
        var context = defaultBackupContextBuilder( backupAddress( defaultDB ) )
                .withDatabaseNamePattern( new DatabaseNamePattern( "*" ) ).build();

        executeBackup( context,
                       new Monitors(),
                       userLogProvider,
                       internalLogProvider
        );

        //then
        assertEquals( defaultDBRepresentation, getBackupDbRepresentation() );
        validateDatabaseRepresentation( natureDB );

        userLogProvider.print( System.out );

        //then verify user and error logs
        validateSuccessfulResult( userLogProvider, defaultDB.databaseName() );
        validateSuccessfulResult( userLogProvider, natureDB );
        LogAssertions.assertThat( internalLogProvider ).forClass( OnlineBackupExecutor.class ).doesNotHaveAnyLogs();

        managementService.shutdown();
    }

    private void validateDatabaseRepresentation( String databaseName )
    {
        var representationInBackupDir = DbRepresentation.of( DatabaseLayout.ofFlat( backupsDir.resolve( databaseName ) ), getConfig() );
        var expectedRepresentation = DbRepresentation.of( managementService.database( databaseName ) );
        assertEquals( expectedRepresentation, representationInBackupDir );
    }

    @TestWithRecordFormats
    void shouldThrowExceptionIfBackupIsNotSuccessfulForAllDBs( String recordFormatName ) throws Exception
    {
        //given
        var defaultDBRepresentation = createInitialDataSet( serverHomeDir, recordFormatName );
        var defaultDB = startDb( serverHomeDir );
        var natureDB = "nature";
        createDatabase( natureDB );
        managementService.shutdownDatabase( natureDB );

        //given log providers
        final var userLogProvider = new AssertableLogProvider();
        final var internalLogProvider = new AssertableLogProvider();

        //when
        var contextBuilder = defaultBackupContextBuilder( backupAddress( defaultDB ) )
                .withDatabaseNamePattern( new DatabaseNamePattern( "n*" ) );

        final var exception = assertThrows( BackupExecutionException.class,
                                            () -> executeBackup( contextBuilder.build(),
                                                                 new Monitors(),
                                                                 userLogProvider,
                                                                 internalLogProvider
                                            ) );

        //then exception is thrown
        assertThat( getRootCause( exception ).getMessage() ).contains( "Not all databases are backed up" );

        //then db representation is the same
        assertEquals( defaultDBRepresentation, getBackupDbRepresentation() );

        //then nature db doesn't exist on file system
        assertFalse( Files.exists( backupsDir.resolve( natureDB ) ) );

        //then verify user and error logs
        validateSuccessfulResult( userLogProvider, defaultDB.databaseName() );
        validateFailedResult( userLogProvider, natureDB, "Request returned an error [Status: 'E_STORE_UNAVAILABLE' " +
                                                         "Message: 'Database 'nature' is stopped. Start the database before backup']" );
        LogAssertions.assertThat( internalLogProvider ).forClass( OnlineBackupExecutor.class )
                     .forLevel( ERROR )
                     .containsMessages( "Error in database " + natureDB );

        managementService.shutdown();
    }

    @TestWithRecordFormats
    void shouldnThrowExceptionIfDatabaseNameWithRegexDoesntMatch( String recordFormatName ) throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverHomeDir, recordFormatName );
        GraphDatabaseService db = startDb( serverHomeDir );

        final var pattern = "t*";
        var contextBuilder = defaultBackupContextBuilder( backupAddress( db ) )
                .withDatabaseNamePattern( new DatabaseNamePattern( pattern ) );

        final var exception = assertThrows( BackupExecutionException.class, () -> executeBackup( contextBuilder.build() ) );
        assertThat( getRootCause( exception ).getMessage() )
                .contains( String.format( "Database name pattern=%s doesn't match any database on the remote server", pattern ) );

        assertNotEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        managementService.shutdown();
    }

    @TestWithRecordFormats
    void shouldReceiveAnExceptionIfBackupOnStoppedDB( String recordFormatName )
    {
        //given
        createInitialDataSet( serverHomeDir, recordFormatName );
        var defaultDB = startDb( serverHomeDir );
        var natureDB = "nature";

        //when
        createDatabase( natureDB );
        managementService.shutdownDatabase( natureDB );

        var contextBuilder = defaultBackupContextBuilder( backupAddress( defaultDB ) ).withDatabaseNamePattern( natureDB );
        var exception = assertThrows( BackupExecutionException.class, () -> executeBackup( contextBuilder.build() ) );

        //then
        assertThat( getRootCause( exception ).getMessage() ).contains( "Database '" + natureDB + "' is stopped" );
    }

    @TestWithRecordFormats
    void shouldThrowExceptionIfDataStoreIdIsDifferentFromServerOne( String recordFormatName ) throws Exception
    {
        createInitialDataSet( serverHomeDir, recordFormatName );
        final var db = startDb( serverHomeDir );

        //when execute full backup
        executeBackup( db );
        managementService.shutdown();

        final var backupToolDir = Neo4jLayout.ofFlat( backupsDir.toAbsolutePath() ).databaseLayout( DEFAULT_DATABASE_NAME ).backupToolsFolder();
        //than change the value in datastore file
        new DatabaseIdStore( fs )
                .writeDatabaseId( DatabaseIdFactory.from( UUID.randomUUID() ), backupToolDir );

        // then add more data and execute incremental backup
        addMoreData( serverHomeDir, recordFormatName );

        final var exception =
                assertThrows( BackupExecutionException.class, () -> executeBackupWithoutFallbackToFull( startDb( serverHomeDir ) ) );
        assertThat( exception.getCause() ).hasMessageContaining( "stored on the file system doesn't match with the server one" );
    }

    @Test
    void shouldBackupSystemDatabaseWithIncludeMetadataAndPrintWarning() throws Exception
    {
        //given
        var defaultDB = startDb( serverHomeDir );
        final var stocksDB = "stocks";
        createDatabase( stocksDB );

        //given log providers
        final var userLogProvider = new AssertableLogProvider();
        final var internalLogProvider = new AssertableLogProvider();

        //when
        var context = defaultBackupContextBuilder( backupAddress( defaultDB ) )
                .withIncludeMetadata( IncludeMetadata.all )
                .withDatabaseNamePattern( new DatabaseNamePattern( "s*" ) )
                .build();

        executeBackup( context, new Monitors(), userLogProvider, internalLogProvider );

        //then validate backup
        validateDatabaseRepresentation( stocksDB );
        assertTrue( fs.fileExists( backupsDir.resolve( SYSTEM_DATABASE_NAME ) ) ); // there is no way to create DbRepresentation from systemDB

        //then validate logs
        validateSuccessfulResult( userLogProvider, stocksDB );
        validateSuccessfulResult( userLogProvider, SYSTEM_DATABASE_NAME );
        LogAssertions.assertThat( userLogProvider ).forClass( OnlineBackupExecutor.class )
                     .forLevel( WARN )
                     .containsMessages( "Include metadata parameter is invalid for backing up system database" );
    }

    @TestWithRecordFormats
    void metadataScriptShouldBeCreatedWhenIncludeMetadataIsDefined( String recordFormatName ) throws Exception
    {
        //given
        createInitialDataSet( serverHomeDir, recordFormatName );
        var defaultDB = startDb( serverHomeDir );
        var natureDB = "nature";
        var natureDBExpectedRepresentation = createDatabase( natureDB );

        var contextBuilder = defaultBackupContextBuilder( backupAddress( defaultDB ) )
                .withDatabaseNamePattern( new DatabaseNamePattern( "nature" ) )
                .withIncludeMetadata( IncludeMetadata.all ).build();

        //when
        executeBackup( contextBuilder );

        //then backup is successful
        validateDatabaseRepresentation( natureDB );

        //and metadata file exists
        final var databaseLayout = DatabaseLayout.ofFlat( backupsDir.resolve( natureDB ) );
        final var metadataFile = MetadataStore.getFilePath( databaseLayout.backupToolsFolder() );
        assertThat( metadataFile ).exists();
        assertThat( fs.getFileSize( metadataFile ) ).isGreaterThan( 0 );
    }

    private void validateSuccessfulResult( AssertableLogProvider userLogProvider, String databaseName )
    {
        LogAssertions.assertThat( userLogProvider ).forClass( OnlineBackupExecutor.class )
                     .forLevel( INFO )
                     .containsMessageWithArguments( "databaseName=%s, backupStatus=%s, reason=%s", databaseName, "successful", "" );
    }

    private void validateFailedResult( AssertableLogProvider userLogProvider, String databaseName, String message )
    {
        LogAssertions.assertThat( userLogProvider ).forClass( OnlineBackupExecutor.class )
                     .forLevel( INFO )
                     .containsMessageWithArguments( "databaseName=%s, backupStatus=%s, reason=%s", databaseName, "failed", message );
    }

    private void createTransactionWithWeirdRelationshipGroupRecord( GraphDatabaseService db )
    {
        Node node;
        RelationshipType typeToDelete = RelationshipType.withName( "A" );
        RelationshipType theOtherType = RelationshipType.withName( "B" );
        int defaultDenseNodeThreshold = dense_node_threshold.defaultValue();

        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( int i = 0; i < defaultDenseNodeThreshold - 1; i++ )
            {
                node.createRelationshipTo( tx.createNode(), theOtherType );
            }
            node.createRelationshipTo( tx.createNode(), typeToDelete );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.getNodeById( node.getId() );
            node.createRelationshipTo( tx.createNode(), theOtherType );
            for ( Relationship relationship : node.getRelationships( Direction.BOTH, typeToDelete ) )
            {
                relationship.delete();
            }
            tx.commit();
        }
    }

    private GraphDatabaseService prepareDatabaseWithTooOldBackup() throws Exception
    {
        GraphDatabaseService db = startDb( serverHomeDir, Maps.mutable.of( keep_logical_logs, SettingValueParsers.FALSE ) );

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
        for ( int i = 0; i < 10; i++ )
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
            for ( Node node : tx.getAllNodes() )
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
        txRepresentation.setHeader( new byte[0], 42, 42, 42, 42, AuthSubject.AUTH_DISABLED );
        TransactionToApply txToApply = new TransactionToApply( txRepresentation, NULL );

        TransactionCommitProcess commitProcess = dependencyResolver( db ).resolveDependency( TransactionCommitProcess.class );
        commitProcess.commit( txToApply, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
    }

    private static ConsistencyCheckService.Result checkConsistency( DatabaseLayout layout ) throws ConsistencyCheckIncompleteException
    {
        Config config = Config.newBuilder()
                              .set( pagecache_memory, "8m" )
                              .set( neo4j_home, layout.getNeo4jLayout().homeDirectory() )
                              .build();

        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();

        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( System.out );
        LogProvider logProvider = new Log4jLogProvider( System.out );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true, true, true );

        return consistencyCheckService.runFullConsistencyCheck( layout, config, progressMonitorFactory, logProvider, true, consistencyFlags );
    }

    private void testTransactionsDuringFullBackup( int nodesInDbBeforeBackup ) throws Exception
    {
        int transactionsDuringBackup = 10;//random.nextInt( 10, 1000 );

        GraphDatabaseService db = startDb( serverHomeDir );
        createIndexAndNodes( db, nodesInDbBeforeBackup );
        long lastCommittedTxIdBeforeBackup = getLastCommittedTx( db );

        Barrier.Control barrier = new Barrier.Control();
        Monitors monitors = dependencyResolver( db ).resolveDependency( Monitors.class );
        monitors.addMonitorListener( new BackupClientPausingMonitor( barrier, serverDatabaseLayout ) );

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

    private void ensureStoresHaveIdFiles( DatabaseLayout databaseLayout )
    {
        for ( Path idFile : databaseLayout.idFiles() )
        {
            assertTrue( Files.exists( idFile ), "Missing id file " + idFile );
        }
    }

    private void assertStoreIsLocked( Path path )
    {
        RuntimeException error = assertThrows( RuntimeException.class, () -> startDb( path ),
                                               "Could build up database in same process, store not locked" );

        assertThat( error.getCause().getCause() ).isInstanceOf( FileLockException.class );
    }

    private DbRepresentation addMoreData( Path path, String recordFormatName )
    {
        GraphDatabaseService db = startDbWithoutOnlineBackup( path, recordFormatName );
        DbRepresentation representation;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( "backup", "Is great" );
            tx.createNode().createRelationshipTo( node, RelationshipType.withName( "LOVES" ) );
            tx.commit();
        }
        finally
        {
            representation = DbRepresentation.of( db );
            managementService.shutdown();
        }
        return representation;
    }

    private DbRepresentation createInitialDataSet( Path path, String recordFormatName )
    {
        GraphDatabaseService db = startDbWithoutOnlineBackup( path, recordFormatName );
        try
        {
            createInitialDataSet( db );
            return DbRepresentation.of( db );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private GraphDatabaseService startDb( Path path )
    {
        return startDb( path, Maps.mutable.of() );
    }

    private GraphDatabaseService startDb( Path path, String recordFormatName )
    {
        return startDb( path, Maps.mutable.of( record_format, recordFormatName ) );
    }

    private GraphDatabaseService startDbWithoutOnlineBackup( Path path )
    {
        Map<Setting<?>,Object> settings = Maps.mutable.of( online_backup_enabled, false,
                                                           record_format, record_format.defaultValue(),
                                                           transaction_logs_root_path, path.toAbsolutePath() );
        return startDb( path, settings );
    }

    private GraphDatabaseService startDbWithoutOnlineBackup( Path path, String recordFormatName )
    {
        Map<Setting<?>,Object> settings = Maps.mutable.of( online_backup_enabled, false, record_format, recordFormatName );
        return startDb( path, settings );
    }

    private GraphDatabaseService startDb( Path path, Map<Setting<?>,Object> settings )
    {
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( path );

        settings.putIfAbsent( databases_root_path, path.toAbsolutePath() );
        settings.putIfAbsent( online_backup_enabled, true );
        settings.putIfAbsent( experimental_catchup_protocol, true );
        builder.setConfig( settings );

        managementService = builder.build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        databases.add( db );
        return db;
    }

    private DbRepresentation getBackupDbRepresentation()
    {
        Config config = getConfig();
        return DbRepresentation.of( backupDatabaseLayout, config );
    }

    private Config getConfig()
    {
        return Config.newBuilder()
                     .set( online_backup_enabled, false )
                     .set( transaction_logs_root_path, backupsDir.toAbsolutePath() )
                     .set( databases_root_path, backupsDir.toAbsolutePath() )
                     .build();
    }

    private DbRepresentation createDatabase( String databaseName )
    {
        managementService.createDatabase( databaseName );
        GraphDatabaseService dbService = managementService.database( databaseName );
        final DbRepresentation representation = DbRepresentation.of( dbService );
        databases.add( dbService );
        return representation;
    }

    private void executeBackup( GraphDatabaseService db ) throws Exception
    {
        executeBackup( backupAddress( db ), new Monitors(), true );
    }

    private void executeBackupWithoutFallbackToFull( GraphDatabaseService db ) throws Exception
    {
        executeBackup( backupAddress( db ), new Monitors(), false );
    }

    private void executeBackup( GraphDatabaseService db, Monitors monitors ) throws Exception
    {
        executeBackup( backupAddress( db ), monitors, true );
    }

    private void executeBackup( String hostname, int port ) throws Exception
    {
        executeBackup( new SocketAddress( hostname, port ), new Monitors(), true );
    }

    private void executeBackup( SocketAddress address, Monitors monitors, boolean fallbackToFull ) throws Exception
    {
        final var contextBuilder = defaultBackupContextBuilder( address )
                .withFallbackToFullBackup( fallbackToFull );
        LogProvider logProvider = new Log4jLogProvider( System.out );

        executeBackup( contextBuilder.build(), monitors, logProvider, logProvider );
    }

    private static void executeBackup( OnlineBackupContext context ) throws Exception
    {
        LogProvider logProvider = new Log4jLogProvider( System.out );
        executeBackup( context, new Monitors(), logProvider, logProvider );
    }

    private static void executeBackup( OnlineBackupContext context,
                                       Monitors monitors,
                                       LogProvider userLog,
                                       LogProvider internalLog ) throws Exception
    {
        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                                                            .withUserLogProvider( userLog )
                                                            .withInternalLogProvider( internalLog )
                                                            .withMonitors( monitors )
                                                            .withClock( Clocks.nanoClock() )
                                                            .build();

        executor.executeBackups( context );
    }

    private OnlineBackupContext.Builder defaultBackupContextBuilder( SocketAddress address )
    {
        Path dir = backupsDir;

        return OnlineBackupContext.builder()
                                  .withAddress( address )
                                  .withBackupDirectory( dir )
                                  .withReportsDirectory( dir )
                                  .withConfig( Config.defaults( experimental_catchup_protocol, true ) )
                                  .withConsistencyCheckRelationshipTypeScanStore( enable_relationship_type_scan_store.defaultValue() );
    }

    private static DbRepresentation addLotsOfData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            int threshold = dense_node_threshold.defaultValue();
            for ( int i = 0; i < threshold * 2; i++ )
            {
                node.createRelationshipTo( tx.createNode(), TEST );
            }
            tx.commit();
        }
        return DbRepresentation.of( db );
    }

    private static void createInitialDataSet( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( Label.label( "Me" ) );
            node.setProperty( "myKey", "myValue" );
            tx.createNode( Label.label( "NotMe" ) ).createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.commit();
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

    private static Node findNodeByLabel( Transaction transaction, Label label )
    {
        try ( ResourceIterator<Node> nodes = transaction.findNodes( label ) )
        {
            return nodes.next();
        }
    }

    private static boolean checkLogFileExistence( Path directory )
    {
        return Files.exists( Config.defaults( logs_directory, directory ).get( store_internal_log_path ) );
    }

    private static long lastTxChecksumOf( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(), Position.LAST_TRANSACTION_CHECKSUM, databaseLayout.getDatabaseName(), NULL );
    }

    private static long getLastCommittedTx( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(), Position.LAST_TRANSACTION_ID, databaseLayout.getDatabaseName(), NULL );
    }

    private static long getLastCommittedTx( GraphDatabaseService db )
    {
        return dependencyResolver( db ).resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private static void setUpgradeTimeInMetaDataStore( DatabaseLayout databaseLayout, PageCache pageCache, long value ) throws IOException
    {
        MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(), Position.UPGRADE_TIME, value, databaseLayout.getDatabaseName(), NULL );
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
            tx.schema().indexFor( Label.label( labelName ) ).on( propertyName ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( LABEL ) ).setProperty( PROPERTY, random.nextString() );
            tx.commit();
        }
    }

    private void checkPreviousCommittedTxIdFromBackupTxLog( long logVersion, long txId ) throws IOException
    {
        // Assert header of specified log version containing correct txId
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDatabaseLayout.databaseDirectory(), fs )
                                           .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                                           .build();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fs, logFiles.getLogFile().getLogFileForVersion( logVersion ), INSTANCE );
        assertEquals( txId, logHeader.getLastCommittedTxId() );
    }

    private String[] findBackupInconsistenciesReports() throws IOException
    {
        try ( Stream<Path> list = Files.list( backupsDir ) )
        {
            return list.map( Path::getFileName )
                       .map( Path::toString )
                       .filter( name -> name.contains( "inconsistencies" ) && name.contains( "report" ) )
                       .toArray( String[]::new );
        }
    }

    private static void rotateAndCheckPoint( GraphDatabaseService db ) throws IOException
    {
        DependencyResolver resolver = dependencyResolver( db );
        resolver.resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
        resolver.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private void flushAndForce( GraphDatabaseService db ) throws IOException
    {
        DependencyResolver resolver = dependencyResolver( db );
        StorageEngine storageEngine = resolver.resolveDependency( StorageEngine.class );
        storageEngine.flushAndForce( NULL );
    }

    private static SocketAddress backupAddress( GraphDatabaseService db )
    {
        DependencyResolver resolver = dependencyResolver( db );
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( BACKUP_SERVER_NAME );
        assertNotNull( address, "Backup server address not registered" );
        return new SocketAddress( address.getHost(), address.getPort() );
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static ExecutorService newSingleThreadedExecutor()
    {
        return Executors.newSingleThreadExecutor( new DaemonThreadFactory() );
    }

    private void checkDatabaseIdCorrectness( String databaseName )
    {
        final var backupToolsFolder = Neo4jLayout.ofFlat( backupsDir ).databaseLayout( databaseName ).backupToolsFolder();
        var databaseId = new DatabaseIdStore( fs )
                .readDatabaseId( backupToolsFolder );
        assertThat( databaseId ).isNotNull();
        var expectedDatabaseId = ((GraphDatabaseFacade) managementService.database( databaseName )).databaseId().databaseId();
        assertEquals( Optional.of( expectedDatabaseId ), databaseId );
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
            if ( file.endsWith( databaseLayout.nodeStore().getFileName().toString() ) ||
                 file.endsWith( databaseLayout.relationshipStore().getFileName().toString() ) )
            {
                barrier.reached(); // multiple calls to this barrier will not block
            }
        }
    }
}
