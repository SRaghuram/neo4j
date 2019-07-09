/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.storeupgrade;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.ServerBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.FileUtils.moveToDirectory;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator.LEGACY_TX_LOGS_LOCATION_SETTING;

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

    @RunWith( Parameterized.class )
    public static class StoreUpgradeTest
    {
        @Parameterized.Parameter( 0 )
        public Store store;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Store[]> stores()
        {
            return Iterables.asCollection( Iterables.concat( STORES34, HIGH_LIMIT_STORES300, HIGH_LIMIT_STORES34 ) );
        }

        @Rule
        public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
        @Rule
        public TestDirectory testDir = TestDirectory.testDirectory();

        @Test
        public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled() throws Throwable
        {
            store.prepareDirectory( testDir.databaseDir() );

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDir.storeDir() );
            builder.setConfig( allow_upgrade, "true" );
            builder.setConfig( logs_directory, testDir.directory( "logs" ).getAbsolutePath() );
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
        }

        @Test
        public void serverDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled() throws Throwable
        {
            File rootDir = testDir.directory();
            DatabaseLayout databaseLayout = DatabaseLayout.of( rootDir, DEFAULT_DATABASE_NAME );

            store.prepareDirectory( databaseLayout.databaseDirectory() );

            File configFile = new File( rootDir, Config.DEFAULT_CONFIG_FILE_NAME );
            Properties props = new Properties();
            props.putAll( ServerTestUtils.getDefaultRelativeProperties() );
            props.setProperty( data_directory.name(), rootDir.getAbsolutePath() );
            props.setProperty( logs_directory.name(), rootDir.getAbsolutePath() );
            props.setProperty( databases_root_path.name(), rootDir.getAbsolutePath() );
            props.setProperty( transaction_logs_root_path.name(), rootDir.getAbsolutePath() );
            props.setProperty( allow_upgrade.name(), "true" );
            props.setProperty( pagecache_memory.name(), "8m" );
            props.setProperty( new HttpConnector( "http" ).type.name(), "HTTP" );
            props.setProperty( new HttpConnector( "http" ).enabled.name(), "true" );
            props.setProperty( new HttpConnector( "http" ).listen_address.name(), "localhost:0" );
            props.setProperty( new HttpConnector( "https" ).enabled.name(), Settings.FALSE );
            props.setProperty( new BoltConnector( "bolt" ).enabled.name(), Settings.FALSE );
            try ( FileWriter writer = new FileWriter( configFile ) )
            {
                props.store( writer, "" );
            }

            ServerBootstrapper bootstrapper = new CommunityBootstrapper();
            try
            {
                bootstrapper.start( rootDir.getAbsoluteFile(), Optional.of( configFile ), Collections.emptyMap() );
                assertTrue( bootstrapper.isRunning() );
                checkInstance( store, bootstrapper.getServer().getDatabaseService().getDatabase() );
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
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            File databaseDir = testDir.databaseDir();
            File transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            File databaseDirectory = store.prepareDirectory( databaseDir );

            // migrated databases have their transaction logs located in
            Set<String> transactionLogFilesBeforeMigration = getTransactionLogFileNames( databaseDirectory, fileSystem );
            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseDirectory.getParentFile() );
            builder.setConfig( allow_upgrade, "true" );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.getAbsolutePath() );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            String startedDatabaseName = database.databaseName();
            managementService.shutdown();

            File newTransactionLogsLocation = new File( transactionLogsRoot, startedDatabaseName );
            assertTrue( fileSystem.fileExists( newTransactionLogsLocation ) );
            Set<String> transactionLogFilesAfterMigration = getTransactionLogFileNames( newTransactionLogsLocation, fileSystem );
            assertEquals( transactionLogFilesBeforeMigration, transactionLogFilesAfterMigration );
        }

        @Test
        public void transactionLogsMovedToConfiguredLocationAfterUpgradeFromCustomLocation() throws IOException
        {
            FileSystemAbstraction fileSystem = testDir.getFileSystem();
            File databaseDir = testDir.databaseDir();
            File transactionLogsRoot = testDir.directory( "transactionLogsRoot" );
            File customTransactionLogsLocation = testDir.directory( "transactionLogsCustom" );
            File databaseDirectory = store.prepareDirectory( databaseDir );
            moveAvailableLogsToCustomLocation( fileSystem, customTransactionLogsLocation, databaseDirectory );

            // migrated databases have their transaction logs located in
            Set<String> transactionLogFilesBeforeMigration = getTransactionLogFileNames( customTransactionLogsLocation, fileSystem );
            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( databaseDirectory.getParentFile() );
            builder.setConfig( allow_upgrade, "true" );
            builder.setConfig( transaction_logs_root_path, transactionLogsRoot.getAbsolutePath() );
            builder.setConfig( LEGACY_TX_LOGS_LOCATION_SETTING, customTransactionLogsLocation.getAbsolutePath() );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            String startedDatabaseName = database.databaseName();
            managementService.shutdown();

            File newTransactionLogsLocation = new File( transactionLogsRoot, startedDatabaseName );
            assertTrue( fileSystem.fileExists( newTransactionLogsLocation ) );
            Set<String> transactionLogFilesAfterMigration = getTransactionLogFileNames( newTransactionLogsLocation, fileSystem );
            assertEquals( transactionLogFilesBeforeMigration, transactionLogFilesAfterMigration );
        }

        private static void moveAvailableLogsToCustomLocation( FileSystemAbstraction fileSystem, File customTransactionLogsLocation, File databaseDirectory )
                throws IOException
        {
            File[] availableTransactionLogFiles = getAvailableTransactionLogFiles( databaseDirectory, fileSystem );
            for ( File transactionLogFile : availableTransactionLogFiles )
            {
                moveToDirectory( transactionLogFile, customTransactionLogsLocation, true );
            }
        }

        private static Set<String> getTransactionLogFileNames( File databaseDirectory, FileSystemAbstraction fileSystem ) throws IOException
        {
            File[] availableLogFilesBeforeMigration = getAvailableTransactionLogFiles( databaseDirectory, fileSystem );
            assertThat( availableLogFilesBeforeMigration, not( emptyArray() ) );

            return stream( availableLogFilesBeforeMigration ).map( File::getName ).collect( toSet() );
        }

        private static File[] getAvailableTransactionLogFiles( File databaseDirectory, FileSystemAbstraction fileSystem ) throws IOException
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDirectory, fileSystem ).build();
            return logFiles.logFiles();
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
            File databaseDirectory = Unzip.unzip( getClass(), dbFileName, testDir.databaseDir() );
            new File( databaseDirectory, "debug.log" ).delete(); // clear the log
            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDir.storeDir() );
            builder.setConfig( allow_upgrade, "true" );
            builder.setConfig( pagecache_memory, "8m" );
            DatabaseManagementService managementService = builder.build();
            GraphDatabaseService databaseService = managementService.database( DEFAULT_DATABASE_NAME );
            try
            {
                DatabaseManager<?> databaseManager = ((GraphDatabaseAPI) databaseService).getDependencyResolver().resolveDependency( DatabaseManager.class );
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( new TestDatabaseIdRepository().defaultDatabase() ).get();
                assertTrue( databaseContext.isFailed() );
                assertThat( databaseContext.failureCause(), new RootCauseMatcher<>( StoreUpgrader.UnexpectedUpgradingStoreVersionException.class ) );
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
            File databaseDirectory = store.prepareDirectory( testDir.databaseDir() );

            // remove id files
            for ( File idFile : DatabaseLayout.of( databaseDirectory ).idFiles() )
            {
                if ( idFile.exists() )
                {
                    assertTrue( idFile.delete() );
                }
            }

            DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDir.storeDir() );
            builder.setConfig( allow_upgrade, "true" );
            builder.setConfig( GraphDatabaseSettings.record_format, store.getFormatFamily() );
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

        File prepareDirectory( File databaseDirectory ) throws IOException
        {
            if ( !databaseDirectory.exists() && !databaseDirectory.mkdirs() )
            {
                throw new IOException( "Could not create directory " + databaseDirectory );
            }
            Unzip.unzip( getClass(), resourceName, databaseDirectory );
            new File( databaseDirectory, "debug.log" ).delete(); // clear the log
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
    }

    private static void checkIndexCounts( Store store, GraphDatabaseAPI db ) throws KernelException
    {
        InwardKernel kernel = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( implicit, AnonymousContext.read() );
              Statement ignore = tx.acquireStatement() )
        {
            SchemaRead schemaRead = tx.schemaRead();
            Iterator<IndexReference> indexes = IndexReference.sortByType( getAllIndexes( schemaRead ) );
            DoubleLongRegister register = Registers.newDoubleLongRegister();
            for ( int i = 0; indexes.hasNext(); i++ )
            {
                IndexReference reference = indexes.next();

                // wait index to be online since sometimes we need to rebuild the indexes on migration
                awaitOnline( schemaRead, reference );

                assertDoubleLongEquals( store.indexCounts[i][0], store.indexCounts[i][1],
                       schemaRead.indexUpdatesAndSize( reference, register ) );
                assertDoubleLongEquals( store.indexCounts[i][2], store.indexCounts[i][3],
                        schemaRead.indexSample( reference, register ) );
                double selectivity = schemaRead.indexUniqueValuesSelectivity( reference );
                assertEquals( store.indexSelectivity[i], selectivity, 0.0000001d );
            }
        }
    }

    private static Iterator<IndexReference> getAllIndexes( SchemaRead schemaRead )
    {
        return schemaRead.indexesGetAll();
    }

    private static void checkLabelCounts( GraphDatabaseAPI db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            HashMap<Label,Long> counts = new HashMap<>();
            for ( Node node : db.getAllNodes() )
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

            ThreadToStatementContextBridge bridge = db.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = bridge.getKernelTransactionBoundToThisThread( true );

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
        try ( Transaction ignored = db.beginTx() )
        {
            ThreadToStatementContextBridge bridge = db.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = bridge.getKernelTransactionBoundToThisThread( true );

            assertThat( kernelTransaction.dataRead().countsForNode( -1 ), is( store.expectedNodeCount ) );
        }
    }

    private static void checkProvidedParameters( Store store, GraphDatabaseAPI db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            // count nodes
            long nodeCount = count( db.getAllNodes() );
            assertThat( nodeCount, is( store.expectedNodeCount ) );

            // count indexes
            long indexCount = count( db.schema().getIndexes() );
            assertThat( indexCount, is( store.indexes() ) );

            // check last committed tx
            TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
            long lastCommittedTxId = txIdStore.getLastCommittedTransactionId();

            try ( Statement statement = db.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .getKernelTransactionBoundToThisThread( true ).acquireStatement() )
            {
                long countsTxId = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                        .testAccessCountsStore().txId();
                assertEquals( lastCommittedTxId, countsTxId );
                assertThat( lastCommittedTxId, is( store.lastTxId ) );
            }
        }
    }

    private static void assertDoubleLongEquals( long expectedFirst, long expectedSecond, DoubleLongRegister register )
    {
        long first = register.readFirst();
        long second = register.readSecond();
        String msg = String.format( "Expected (%d,%d), got (%d,%d)", expectedFirst, expectedSecond, first, second );
        assertEquals( msg, expectedFirst, first );
        assertEquals( msg, expectedSecond, second );
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

    private static void awaitOnline( SchemaRead schemRead, IndexReference index )
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
}
