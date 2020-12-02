/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.storeupgrade;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetV4_0;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogVersions;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesSpan;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoBootstrapper;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.FileUtils.moveToDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.server.WebContainerTestUtils.getDefaultRelativeProperties;

@RunWith( Enclosed.class )
public class StoreUpgradeIT
{
    // NOTE: the zip files must contain the databases files and NOT the database folder itself!!!
    private static final List<Store[]> STORES34 = Arrays.asList(
            new Store[]{new Store( "0.A.9-empty.zip",
                    0 /* node count */,
                    1 /* last txId */,
                    selectivities(),
                    indexCounts()
            )},
            new Store[]{new Store( "0.A.9-data.zip",
                    174 /* node count */,
                    30 /* last txId */,
                    selectivities( 1.0, 1.0, 1.0 ),
                    indexCounts( counts( 0, 38, 38, 38 ), counts( 0, 1, 1, 1 ), counts( 0, 133, 133, 133 ) )
            )} );
    private static final List<Store[]> HIGH_LIMIT_STORES34 = Arrays.asList(
            new Store[]{new Store( "E.H.4-empty.zip",
                    0 /* node count */,
                    1 /* last txId */,
                    selectivities(),
                    indexCounts(),
                    HighLimit.NAME
                    )},
            new Store[]{new Store( "E.H.4-data.zip",
                    174 /* node count */,
                    30 /* last txId */,
                    selectivities( 1.0, 1.0, 1.0 ),
                    indexCounts( counts( 0, 38, 38, 38 ), counts( 0, 1, 1, 1 ), counts( 0, 133, 133, 133 ) ),
                    HighLimit.NAME
                    )} );
    private static final List<Store[]> HIGH_LIMIT_STORES300 = Arrays.asList(
            new Store[]{new Store( "E.H.0-empty.zip",
                    0 /* node count */,
                    1 /* last txId */,
                    selectivities(),
                    indexCounts(),
                    HighLimit.NAME
            )},
            new Store[]{new Store( "E.H.0-data.zip",
                    174 /* node count */,
                    30 /* last txId */,
                    selectivities( 1.0, 1.0, 1.0 ),
                    indexCounts( counts( 0, 38, 38, 38 ), counts( 0, 1, 1, 1 ), counts( 0, 133, 133, 133 ) ),
                    HighLimit.NAME
            )} );
    private static final List<Store[]> STORES40 = Arrays.asList(
            new Store[]{new Store( "0.0.4FS-empty.zip",
                    0 /* node count */,
                    1 /* last txId */,
                    selectivities(),
                    indexCounts()
            )},
            new Store[]{new Store( "0.0.4FS-data.zip",
                    174 /* node count */,
                    34 /* last txId */,
                    selectivities( 1.0, 1.0, 1.0, 1.0, 1.0, 1.0 ),
                    indexCounts( counts( 0, 38, 38, 38 ), counts( 0, 1, 1, 1 ),
                            counts( 0, 0, 0, 0 ), counts( 0, 0, 0, 0 ),
                            counts( 0, 0, 0, 0 ), counts( 0, 133, 133, 133 ) )
            )} );

    @RunWith( Parameterized.class )
    public static class StoreUpgradeTest
    {
        @Parameterized.Parameter( 0 )
        public Store store;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Store[]> stores()
        {
            return Iterables.asCollection( Iterables.concat( STORES34, HIGH_LIMIT_STORES300, HIGH_LIMIT_STORES34, STORES40 ) );
        }

        @Rule
        public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
        @Rule
        public TestDirectory testDir = TestDirectory.testDirectory();

        @Test
        public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled() throws Throwable
        {
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( layout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            DatabaseLayout databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                managementService.shutdown();
            }

            assertConsistentStore( databaseLayout );

            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".a" ) ) );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".b" ) ) );
            assertFalse( Files.exists( layout.file( BadCollector.BAD_FILE_NAME ) ) );
        }

        @Test
        public void embeddedDatabaseShouldUpgradeViaDatabaseManager() throws Exception
        {
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( layout );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());

            DatabaseManagementService managementService = builder.build();
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            NamedDatabaseId namedDatabaseId = database.databaseId();
            DefaultDatabaseManager databaseManager = database.getDependencyResolver().resolveDependency( DefaultDatabaseManager.class );
            databaseManager.upgradeDatabase( namedDatabaseId );

            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            DatabaseLayout databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                managementService.shutdown();
            }

            assertConsistentStore( databaseLayout );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".a" ) ) );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".b" ) ) );
        }

        @Test
        public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabledDynamically() throws Throwable
        {
            assumeStoreNot4_0();
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( layout );
            builder.setConfig( allow_upgrade, false );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());
            DatabaseManagementService managementService = builder.build();
            var defaultDatabase = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            var db = defaultDatabase.getDependencyResolver().resolveDependency( Database.class );

            var tempDatabaseName = "tempdatabase";
            managementService.createDatabase( tempDatabaseName );
            var tempDb = managementService.database( tempDatabaseName );

            // not migrated and failed
            assertFalse( defaultDatabase.isAvailable( 0 ) );
            var availabilityGuard = defaultDatabase.getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class );
            var availabilityException = getUnavailableException( availabilityGuard );
            assertThat( availabilityException ).hasRootCauseInstanceOf( UpgradeNotAllowedException.class );

            // now upgrade should be allowed
            tempDb.executeTransactionally( "call dbms.setConfigValue('" + allow_upgrade.name() + "', 'true')" );

            // restart database
            db.stop();
            db.start();

            // now it should be upgraded and available
            defaultDatabase = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            assertTrue( defaultDatabase.isAvailable( MINUTES.toMillis( 1 ) ) );

            DatabaseLayout databaseLayout = defaultDatabase.databaseLayout();
            try
            {
                checkInstance( store, defaultDatabase );
            }
            finally
            {
                db.stop();
                managementService.shutdown();
            }

            assertConsistentStore( databaseLayout );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".a" ) ) );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".b" ) ) );
        }

        @Test
        public void mustBeAbleToCreateTokensAfterUpgrade() throws Throwable
        {
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( layout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode( Label.label( "NonExistentLabel_82736482736" ) );
                    node.setProperty( "nonExistentProperty_987348526435876", 1 );
                    node.createRelationshipTo( node, RelationshipType.withName( "NonExistentType_5392436583" ) );
                    tx.commit();
                }
            }
            finally
            {
                managementService.shutdown();
            }

            managementService = builder.build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode( Label.label( "NonExistentLabel_09813492873" ) );
                    node.setProperty( "nonExistentProperty_134587645837", 1 );
                    node.createRelationshipTo( node, RelationshipType.withName( "NonExistentType_39485293845638" ) );
                    tx.commit();
                }
            }
            finally
            {
                managementService.shutdown();
            }
            DatabaseLayout databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            assertConsistentStore( databaseLayout );
        }

        @Test
        public void mustBeAbleToCreateSchemaAfterUpgrade() throws Throwable
        {
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( layout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( Label.label( "NonExistentLabel_82736482736" ) ).on( "nonExistentProperty_987348526435876" ).create();
                    tx.commit();
                }
            }
            finally
            {
                managementService.shutdown();
            }

            managementService = builder.build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( Label.label( "NonExistentLabel_09813492873" ) ).on( "nonExistentProperty_134587645837" ).create();
                    tx.commit();
                }
            }
            finally
            {
                managementService.shutdown();
            }
            DatabaseLayout databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            assertConsistentStore( databaseLayout );
        }

        @Test
        public void serverDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled() throws Throwable
        {
            Path rootDir = testDir.homePath();
            DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( testDir.directory( DEFAULT_DATABASE_NAME ) );

            store.prepareDirectory( databaseLayout.databaseDirectory() );

            Path configFile = rootDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME );
            Properties props = new Properties();
            props.putAll( getDefaultRelativeProperties( rootDir ) );
            props.setProperty( data_directory.name(), rootDir.toAbsolutePath().toString() );
            props.setProperty( logs_directory.name(), rootDir.toAbsolutePath().toString() );
            props.setProperty( databases_root_path.name(), rootDir.toAbsolutePath().toString() );
            props.setProperty( transaction_logs_root_path.name(), rootDir.toAbsolutePath().toString() );
            props.setProperty( allow_upgrade.name(), TRUE );
            props.setProperty( fail_on_missing_files.name(), FALSE );
            props.setProperty( pagecache_memory.name(), "8m" );
            props.setProperty( HttpConnector.enabled.name(), TRUE );
            props.setProperty( HttpConnector.listen_address.name(), "localhost:0" );
            props.setProperty( HttpsConnector.enabled.name(), FALSE );
            props.setProperty( BoltConnector.enabled.name(), FALSE );
            props.setProperty( GraphDatabaseSettings.preallocate_logical_logs.name(), FALSE );
            try ( Writer writer = Files.newBufferedWriter( configFile, StandardCharsets.UTF_8 ) )
            {
                props.store( writer, "" );
            }

            NeoBootstrapper bootstrapper = new CommunityBootstrapper();
            try
            {
                bootstrapper.start( rootDir.toAbsolutePath(), configFile, Collections.emptyMap(), false );
                assertTrue( bootstrapper.isRunning() );
                checkInstance( store, (GraphDatabaseAPI) bootstrapper.getDatabaseManagementService().database( DEFAULT_DATABASE_NAME ) );
            }
            finally
            {
                bootstrapper.stop();
            }

            assertConsistentStore( databaseLayout );
        }

        @Test
        public void transactionLogsMovedToConfiguredLocationAfterUpgrade() throws IOException
        {
            assumeStoreNot4_0();
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            Path transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            Path databaseDirectory = store.prepareDirectory( databaseDir );

            // migrated databases have their transaction logs located in
            Set<String> transactionLogFilesBeforeMigration = getTransactionLogFileNames( databaseDirectory, fileSystem );
            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            String startedDatabaseName = database.databaseName();
            managementService.shutdown();

            Path newTransactionLogsLocation = transactionLogsRoot.resolve( startedDatabaseName );
            assertTrue( fileSystem.fileExists( newTransactionLogsLocation ) );
            Set<String> transactionLogFilesAfterMigration = getTransactionLogFileNames( newTransactionLogsLocation, fileSystem );
            assertThat( transactionLogFilesAfterMigration ).containsAll( transactionLogFilesBeforeMigration );
        }

        private void assumeStoreNot4_0()
        {
            assumeFalse( STORES40.stream().flatMap( Stream::of ).anyMatch( s -> s == store ) );
        }

        @Test
        public void transactionLogsMovedToConfiguredLocationAfterUpgradeFromCustomLocation() throws IOException
        {
            assumeStoreNot4_0();
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            Path transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            Path customTransactionLogsLocation = testDir.directory( "transactionLogsCustom" );
            Path databaseDirectory = store.prepareDirectory( databaseDir );
            moveAvailableLogsToCustomLocation( fileSystem, customTransactionLogsLocation, databaseDirectory );

            // migrated databases have their transaction logs located in
            Set<String> transactionLogFilesBeforeMigration = getTransactionLogFileNames( customTransactionLogsLocation, fileSystem );
            TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDir.homePath() );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() );
            builder.setConfig( GraphDatabaseInternalSettings.logical_logs_location, customTransactionLogsLocation.toAbsolutePath() );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            String startedDatabaseName = database.databaseName();
            managementService.shutdown();

            Path newTransactionLogsLocation = transactionLogsRoot.resolve( startedDatabaseName );
            assertTrue( fileSystem.fileExists( newTransactionLogsLocation ) );
            Set<String> transactionLogFilesAfterMigration = getTransactionLogFileNames( newTransactionLogsLocation, fileSystem );
            assertThat( transactionLogFilesAfterMigration ).containsAll( transactionLogFilesBeforeMigration );
        }

        @Test
        public void upgradeMustInjectEmptyTransactionIfTransactionLogsAreEmpty() throws Exception
        {
            assumeStoreNot4_0();
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            Path transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            store.prepareDirectory( databaseDir );

            // Remove everything from the log file, except the header and last check point.
            try ( JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                 PageCache pageCache = StandalonePageCacheFactory.createPageCache( testDir.getFileSystem(), scheduler ) )
            {
                LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDir, fileSystem ).build();
                LogFile logFile = logFiles.getLogFile();
                assertEquals( "This test only anticipates a single log file. " +
                                "Please update if a test database has more than one log file.",
                        logFile.getLowestLogVersion(), logFile.getHighestLogVersion() );
                LongSupplier lastCommittedTxId = () ->
                {
                    try
                    {
                        return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(), MetaDataStore.Position.LAST_TRANSACTION_ID,
                                PageCursorTracer.NULL );
                    }
                    catch ( IOException e )
                    {
                        throw new UncheckedIOException( e );
                    }
                };
                long version = logFile.getHighestLogVersion();
                try ( PhysicalLogVersionedStoreChannel channel = logFile.createLogChannelForVersion( version, lastCommittedTxId ) )
                {
                    byte logFormatVersion = channel.getLogFormatVersion();
                    switch ( logFormatVersion )
                    {
                        case LogVersions.LOG_VERSION_3_5:
                            channel.truncate( LogVersions.LOG_HEADER_SIZE_3_5 );
                            break;
                        case LogVersions.LOG_VERSION_4_0:
                            channel.truncate( LogVersions.LOG_HEADER_SIZE_4_0 );
                            break;
                        default:
                            throw new RuntimeException( "Unexpected log format version: " + logFormatVersion );
                    }
                    try ( NativeScopedBuffer buffer = new NativeScopedBuffer( 100, EmptyMemoryTracker.INSTANCE ) )
                    {
                        var writableChannel = new PositionAwarePhysicalFlushableChecksumChannel( channel, buffer );
                        var entryWriter = new LogEntryWriter( writableChannel, LogEntryParserSetV4_0.V4_0 );
                        entryWriter.writeLegacyCheckPointEntry( new LogPosition( version, channel.position() ) );
                        writableChannel.prepareForFlush().flush();
                    }
                }
            }

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() );
            DatabaseManagementService managementService = builder.build();
            managementService.database( DEFAULT_DATABASE_NAME );
            managementService.shutdown();

            Config config = Config.defaults( Map.of(
                    neo4j_home, testDir.homePath(),
                    transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() ) );
            DatabaseLayout layout  = Neo4jLayout.of( config ).databaseLayout( DEFAULT_DATABASE_NAME );
            try ( LogFilesSpan span = getAccessibleLogFiles( layout ) )
            {
                LogFiles logFiles = span.getLogFiles();
                LogFile logFile = logFiles.getLogFile();
                LogPosition startPosition = logFile.extractHeader( logFile.getHighestLogVersion() ).getStartPosition();
                boolean foundStartEntry = false;
                boolean foundCommitEntry = false;
                List<LogEntry> entries = new ArrayList<>();
                try ( ReadableLogChannel channel = logFile.getReader( startPosition ) )
                {
                    VersionAwareLogEntryReader reader = new VersionAwareLogEntryReader(
                            StorageEngineFactory.selectStorageEngine().commandReaderFactory() );
                    LogEntry logEntry;
                    while ( ( logEntry = reader.readLogEntry( channel ) ) != null )
                    {
                        entries.add( logEntry );
                        foundStartEntry |= logEntry.getType() == LogEntryTypeCodes.TX_START;
                        foundCommitEntry |= logEntry.getType() == LogEntryTypeCodes.TX_COMMIT;
                    }
                }
                assertTrue( "Expected to find a transaction start entry. Actual entries: " + entries, foundStartEntry );
                assertTrue( "Expected to find a transaction commit entry. Actual entries: " + entries, foundCommitEntry );
            }
        }

        @Test
        public void upgradeMustInjectEmptyTransactionIfTransactionLogsAreAllowedToBeMissing() throws Exception
        {
            assumeStoreNot4_0();
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            Path transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            store.prepareDirectory( databaseDir );

            // Remove all log files.
            TransactionLogFilesHelper helper = new TransactionLogFilesHelper( fileSystem, databaseDir );
            Path[] logs = helper.getMatchedFiles();
            assertNotNull( "Expected some log files to exist.", logs );
            for ( Path logFile : logs )
            {
                fileSystem.deleteFile( logFile );
            }

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() );

            // Important configuration: explicitly allow log files to be missing!
            builder.setConfig( fail_on_missing_files, false );

            DatabaseManagementService managementService = builder.build();
            try
            {
                GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
                database.beginTx().close(); // Check the database really started.
            }
            finally
            {
                managementService.shutdown();
            }

            Config config = Config.defaults( Map.of(
                    neo4j_home, testDir.homePath(),
                    transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() ) );
            DatabaseLayout layout  = Neo4jLayout.of( config ).databaseLayout( DEFAULT_DATABASE_NAME );
            try ( LogFilesSpan span = getAccessibleLogFiles( layout ) )
            {
                LogFiles logFiles = span.getLogFiles();
                LogFile logFile = logFiles.getLogFile();
                LogPosition startPosition = logFile.extractHeader( logFile.getHighestLogVersion() ).getStartPosition();
                boolean foundStartEntry = false;
                boolean foundCommitEntry = false;
                List<LogEntry> entries = new ArrayList<>();
                try ( ReadableLogChannel channel = logFile.getReader( startPosition ) )
                {
                    VersionAwareLogEntryReader reader = new VersionAwareLogEntryReader(
                            StorageEngineFactory.selectStorageEngine().commandReaderFactory() );
                    LogEntry logEntry;
                    while ( ( logEntry = reader.readLogEntry( channel ) ) != null )
                    {
                        entries.add( logEntry );
                        foundStartEntry |= logEntry.getType() == LogEntryTypeCodes.TX_START;
                        foundCommitEntry |= logEntry.getType() == LogEntryTypeCodes.TX_COMMIT;
                    }
                }
                assertTrue( "Expected to find a transaction start entry. Actual entries: " + entries, foundStartEntry );
                assertTrue( "Expected to find a transaction commit entry. Actual entries: " + entries, foundCommitEntry );
            }
        }

        @Test
        public void mustRefuseUpgradeByDefaultIfLogFilesAreMissing() throws Exception
        {
            assumeStoreNot4_0();
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            Path transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            store.prepareDirectory( databaseDir );

            // Remove all log files.
            TransactionLogFilesHelper helper = new TransactionLogFilesHelper( fileSystem, databaseDir );
            Path[] logs = helper.getMatchedFiles();
            assertNotNull( "Expected some log files to exist.", logs );
            for ( Path logFile : logs )
            {
                fileSystem.deleteFile( logFile );
            }

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.toAbsolutePath() );

            // Note: we don't set 'fail_on_missing_files' to 'false'.
            // It is 'true' by default, so upgrade should fail when log files are missing.

            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                database.beginTx().close();
                fail( "Expected 'beginTx' to fail." );
            }
            catch ( Exception e )
            {
                assertThat( e.getCause() ).isInstanceOf( StoreUpgrader.DatabaseNotCleanlyShutDownException.class );
            }
            finally
            {
                managementService.shutdown();
            }
        }

        private static void moveAvailableLogsToCustomLocation( FileSystemAbstraction fileSystem, Path customTransactionLogsLocation, Path databaseDirectory )
                throws IOException
        {
            Path[] availableTransactionLogFiles = getAvailableTransactionLogFiles( databaseDirectory, fileSystem );
            for ( Path transactionLogFile : availableTransactionLogFiles )
            {
                moveToDirectory( transactionLogFile.toFile(), customTransactionLogsLocation.toFile(), true );
            }
        }

        private static Set<String> getTransactionLogFileNames( Path databaseDirectory, FileSystemAbstraction fileSystem ) throws IOException
        {
            Path[] availableTransactionLogFiles = getAvailableTransactionLogFiles( databaseDirectory, fileSystem );
            assertThat( availableTransactionLogFiles ).isNotEmpty();

            return stream( availableTransactionLogFiles ).map( Path::getFileName ).map( Path::toString ).collect( toSet() );
        }

        private static Path[] getAvailableTransactionLogFiles( Path databaseDirectory, FileSystemAbstraction fileSystem ) throws IOException
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDirectory, fileSystem ).build();
            return logFiles.logFiles();
        }

        private LogFilesSpan getAccessibleLogFiles( DatabaseLayout layout ) throws IOException
        {
            JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
            PageCache pageCache = StandalonePageCacheFactory.createPageCache( testDir.getFileSystem(), scheduler );
            PageCursorTracer cursorTracer = PageCursorTracer.NULL;

            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
            LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( layout, fileSystem, pageCache )
                                  .withStoreId( storageEngineFactory.storeId( layout, pageCache, cursorTracer ) )
                                  .withLogVersionRepository( storageEngineFactory.readOnlyLogVersionRepository(
                                          layout, pageCache, cursorTracer ) )
                                  .withTransactionIdStore( storageEngineFactory.readOnlyTransactionIdStore(
                                          fileSystem, layout, pageCache, cursorTracer ) )
                                  .build();

            Lifespan lifespan = new Lifespan( scheduler, LifecycleAdapter.onShutdown( pageCache::close ), logFiles );
            return new LogFilesSpan( lifespan, logFiles );
        }
    }

    @RunWith( Parameterized.class )
    public static class StoreUpgradeNotRequiredTest
    {
        @Rule
        public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
        @Rule
        public TestDirectory testDir = TestDirectory.testDirectory();

        @Parameterized.Parameter( 0 )
        public Store store;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Store[]> stores()
        {
            return Iterables.asCollection( Iterables.concat( STORES40 ) );
        }

        @Test
        public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled() throws Throwable
        {
            var layout = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            store.prepareDirectory( layout.databaseDirectory() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( layout );
            builder.setConfig( allow_upgrade, false );
            builder.setConfig( fail_on_missing_files, false );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).toAbsolutePath());
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            DatabaseLayout databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                managementService.shutdown();
            }

            assertConsistentStore( databaseLayout );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".a" ) ) );
            assertFalse( Files.exists( layout.countStore().resolveSibling( layout.countStore().getFileName() + ".b" ) ) );
        }
    }

    @RunWith( Parameterized.class )
    public static class StoreUpgradeFailingTest
    {
        @Rule
        public final TestDirectory testDir = TestDirectory.testDirectory();

        @Parameterized.Parameter( 0 )
        public String ignored; // to make JUnit happy...
        @Parameterized.Parameter( 1 )
        public String dbFileName;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String[]> parameters()
        {
            return Arrays.asList(
                    new String[]{"on a not cleanly shutdown database", "0.A.3-to-be-recovered.zip"},
                    new String[]{"on a 1.9 store", "0.A.0-db.zip"},
                    new String[]{"on a 2.0 store", "0.A.1-db.zip"},
                    new String[]{"on a 2.1 store", "0.A.3-data.zip"},
                    new String[]{"on a 2.2 store", "0.A.5-data.zip"}
            );
        }

        @Test
        public void migrationShouldFail() throws Throwable
        {
            // migrate the store using a single instance

            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();
            testDir.getFileSystem().mkdirs( databaseDir );
            Path databaseDirectory = Unzip.unzip( getClass(), dbFileName, databaseDir );
            Path debugLog = databaseDirectory.resolve( "debug.log" );
            if ( Files.exists( debugLog ) )
            {
                Files.delete( debugLog ); // clear the log
            }
            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( pagecache_memory, "8m" );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                DatabaseStateService dbStateService = database.getDependencyResolver().resolveDependency( DatabaseStateService.class );
                var failure = dbStateService.causeOfFailure( database.databaseId() );
                assertTrue( failure.isPresent() );
                assertThat( failure.get() ).hasRootCauseInstanceOf( StoreUpgrader.UnexpectedUpgradingStoreVersionException.class );
            }
            finally
            {
                managementService.shutdown();
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class StoreWithoutIdFilesUpgradeTest
    {
        @Parameterized.Parameter( 0 )
        public Store store;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Store[]> stores()
        {
            return Iterables.asCollection( Iterables.concat( STORES34, HIGH_LIMIT_STORES300, HIGH_LIMIT_STORES34 ) );
        }

        @Rule
        public final TestDirectory testDir = TestDirectory.testDirectory();

        @Test
        public void shouldBeAbleToUpgradeAStoreWithoutIdFilesAsBackups() throws Throwable
        {
            DatabaseLayout databaseLayout  = Neo4jLayout.of( testDir.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
            Path databaseDir = databaseLayout.databaseDirectory();

            Path databaseDirectory = store.prepareDirectory( databaseDir );

            // remove id files
            for ( Path idFile : DatabaseLayout.ofFlat( databaseDirectory ).idFiles() )
            {
                if ( Files.exists( idFile ) )
                {
                    Files.delete( idFile );
                }
            }

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseLayout );
            builder.setConfig( allow_upgrade, true );
            builder.setConfig( GraphDatabaseSettings.record_format, store.getFormatFamily() );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                managementService.shutdown();
            }

            assertConsistentStore( databaseLayout );
        }
    }

    private static class Store
    {
        private final String resourceName;
        final long expectedNodeCount;
        final long lastTxId;
        private final double[] indexSelectivity;
        final long[][] indexCounts;
        private final String formatFamily;

        private Store( String resourceName, long expectedNodeCount, long lastTxId,
                double[] indexSelectivity, long[][] indexCounts )
        {
            this( resourceName, expectedNodeCount, lastTxId, indexSelectivity, indexCounts, Standard.LATEST_NAME );
        }

        private Store( String resourceName, long expectedNodeCount, long lastTxId,
                double[] indexSelectivity, long[][] indexCounts, String formatFamily )
        {
            this.resourceName = resourceName;
            this.expectedNodeCount = expectedNodeCount;
            this.lastTxId = lastTxId;
            this.indexSelectivity = indexSelectivity;
            this.indexCounts = indexCounts;
            this.formatFamily = formatFamily;
        }

        Path prepareDirectory( Path databaseDirectory ) throws IOException
        {
            if ( Files.notExists( databaseDirectory ) )
            {
                Files.createDirectories( databaseDirectory );
            }
            Unzip.unzip( getClass(), resourceName, databaseDirectory );
            Path debugLog = databaseDirectory.resolve( "debug.log" );
            if ( Files.exists( debugLog ) )
            {
                Files.delete( debugLog ); // clear the log
            }
            return databaseDirectory;
        }

        @Override
        public String toString()
        {
            return "Store: " + resourceName;
        }

        long indexes()
        {
            return indexCounts.length;
        }

        String getFormatFamily()
        {
            return formatFamily;
        }
    }

    private static void checkInstance( Store store, GraphDatabaseAPI db ) throws KernelException
    {
        checkProvidedParameters( store, db );
        checkGlobalNodeCount( store, db );
        checkLabelCounts( db );
        checkIndexCounts( store, db );
        checkConstraints( db );
    }

    private static void checkIndexCounts( Store store, GraphDatabaseAPI db ) throws KernelException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( IMPLICIT, AnonymousContext.read() ) )
        {
            SchemaRead schemaRead = tx.schemaRead();
            Iterator<IndexDescriptor> indexes = IndexDescriptor.sortByType( getAllIndexes( schemaRead ) );
            for ( int i = 0; indexes.hasNext(); i++ )
            {
                IndexDescriptor reference = indexes.next();

                // wait index to be online since sometimes we need to rebuild the indexes on migration
                awaitOnline( schemaRead, reference );

                var indexSample = schemaRead.indexSample( reference );
                assertEquals( store.indexCounts[i][0], indexSample.updates() );
                assertEquals( store.indexCounts[i][1], indexSample.indexSize() );
                assertEquals( store.indexCounts[i][2], indexSample.uniqueValues() );
                assertEquals( store.indexCounts[i][3], indexSample.sampleSize() );

                double selectivity = schemaRead.indexUniqueValuesSelectivity( reference );
                assertEquals( store.indexSelectivity[i], selectivity, 0.0000001d );
            }
        }
    }

    private static void checkConstraints( GraphDatabaseAPI db )
    {
        // All constraints that have indexes, must have their indexes named after them.
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : tx.schema().getConstraints() )
            {
                if ( constraint.isConstraintType( ConstraintType.UNIQUENESS ) || constraint.isConstraintType( ConstraintType.NODE_KEY ) )
                {
                    // These constraints have indexes, so we must be able to find their indexes by the constraint name.
                    // The 'getIndexByName' method will throw if there is no such index.
                    tx.schema().getIndexByName( constraint.getName() );
                }
            }
            tx.commit();
        }
    }

    private static Iterator<IndexDescriptor> getAllIndexes( SchemaRead schemaRead )
    {
        return schemaRead.indexesGetAll();
    }

    private static void checkLabelCounts( GraphDatabaseAPI db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            HashMap<Label,Long> counts = new HashMap<>();
            for ( Node node : transaction.getAllNodes() )
            {
                for ( Label label : node.getLabels() )
                {
                    Long count = counts.get( label );
                    if ( count != null )
                    {
                        counts.put( label, count + 1 );
                    }
                    else
                    {
                        counts.put( label, 1L );
                    }
                }
            }

            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();

            for ( Map.Entry<Label,Long> entry : counts.entrySet() )
            {
                assertEquals(
                        entry.getValue().longValue(),
                        kernelTransaction.dataRead().countsForNode(
                                kernelTransaction.tokenRead().nodeLabel( entry.getKey().name() ) )
                );
            }
        }
    }

    private static void checkGlobalNodeCount( Store store, GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction kernelTransaction = ((InternalTransaction) tx).kernelTransaction();

            assertThat( kernelTransaction.dataRead().countsForNode( -1 ) ).isEqualTo( store.expectedNodeCount );
        }
    }

    private static void checkProvidedParameters( Store store, GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            // count nodes
            long nodeCount = count( tx.getAllNodes() );
            assertThat( nodeCount ).isEqualTo( store.expectedNodeCount );

            // count indexes
            long indexCount = count( tx.schema().getIndexes() );
            assertThat( indexCount ).isEqualTo( store.indexes() );

            // check last committed tx
            TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
            long lastCommittedTxId = txIdStore.getLastCommittedTransactionId();

            GBPTreeCountsStore countsStore = db.getDependencyResolver().resolveDependency( GBPTreeCountsStore.class );
            long countsTxId = countsStore.txId();
            assertEquals( lastCommittedTxId, countsTxId );
            assertThat( lastCommittedTxId ).isEqualTo( store.lastTxId );
        }
    }

    private static double[] selectivities( double... selectivity )
    {
        return selectivity;
    }

    private static long[][] indexCounts( long[]... counts )
    {
        return counts;
    }

    private static long[] counts( long upgrade, long size, long unique, long sampleSize )
    {
        return new long[]{upgrade, size, unique, sampleSize};
    }

    private static void awaitOnline( SchemaRead schemRead, IndexDescriptor index )
            throws KernelException
    {
        long start = System.currentTimeMillis();
        long end = start + 20_000;
        while ( System.currentTimeMillis() < end )
        {
            switch ( schemRead.indexGetState( index ) )
            {
            case ONLINE:
                return;

            case FAILED:
                throw new IllegalStateException( "Index failed instead of becoming ONLINE" );

            default:
                break;
            }

            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                // ignored
            }
        }
        throw new IllegalStateException( "Index did not become ONLINE within reasonable time" );
    }

    private static DatabaseShutdownException getUnavailableException( DatabaseAvailabilityGuard availabilityGuard ) throws UnavailableException
    {
        try
        {
            availabilityGuard.assertDatabaseAvailable();
        }
        catch ( DatabaseShutdownException expected )
        {
            return expected;
        }
        throw new AssertionError( "Expected to get availability exceptions above" );
    }
}
