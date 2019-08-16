/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.IdFilesDeleter;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.EnterpriseTemporaryDatabaseFactory;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenState;
import com.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.helpers.ClassicNeo4jDatabase;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.internal.recordstorage.ReadOnlyTransactionIdStore;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_STORE_VERSION;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@PageCacheExtension
class RaftBootstrapperIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private static final DatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();

    private final DatabaseInitializer databaseInitializer = DatabaseInitializer.NO_INITIALIZATION;
    private final Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
    private StoreId storeId = new StoreId( MetaDataStore.versionStringToLong( LATEST_STORE_VERSION ) );

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final Monitors monitors = new Monitors();

    private TemporaryDatabaseFactory temporaryDatabaseFactory;

    private File neo4jHome;
    private File dataDirectory;
    private File storeDirectory; // "databases"
    private File txLogsDirectory;
    private Config defaultConfig;
    private StorageEngineFactory storageEngineFactory;

    @BeforeEach
    void setup()
    {
        this.temporaryDatabaseFactory = new EnterpriseTemporaryDatabaseFactory( pageCache );
        this.neo4jHome = testDirectory.directory();
        this.dataDirectory = new File( neo4jHome, DEFAULT_DATA_DIR_NAME );
        this.storeDirectory = new File( dataDirectory, DEFAULT_DATABASES_ROOT_DIR_NAME );
        this.txLogsDirectory = new File( dataDirectory, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        this.defaultConfig = Config.defaults( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath() );
        this.storageEngineFactory = StorageEngineFactory.selectStorageEngine();
    }

    @Test
    void shouldBootstrapWhenNoDirectoryExists() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory, () -> of( txLogsDirectory ), DEFAULT_DATABASE_NAME );
        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership, storeId );

        // then
        verifySnapshot( snapshot, membership, defaultConfig );
    }

    @Test
    void shouldBootstrapWhenEmptyDirectoryExists() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory, () -> of( txLogsDirectory ), DEFAULT_DATABASE_NAME );
        fileSystem.mkdirs( databaseLayout.databaseDirectory() );
        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership, storeId );

        // then
        verifySnapshot( snapshot, membership, defaultConfig );
    }

    @Test
    void shouldBootstrapFromSeed() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership, storeId );

        // then
        verifySnapshot( snapshot, membership, defaultConfig );
    }

    @Test
    void shouldBootstrapWithCustomTransactionLogsLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        File customTransactionLogsRootDirectory = testDirectory.directory( "custom-tx-logs-location" );
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .transactionLogsRootDirectory( customTransactionLogsRootDirectory )
                .build();

        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath() )
                .set( transaction_logs_root_path, customTransactionLogsRootDirectory.toPath().toAbsolutePath() )
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, logProvider, storageEngineFactory, config );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership, storeId );

        // then
        verifySnapshot( snapshot, membership, config );
    }

    @Test
    void shouldFailToBootstrapWithDeletedIdFiles() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .build();

        IdFilesDeleter.deleteIdFiles( database.layout(), fileSystem );

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig );

        // when
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( membership, storeId ) );

        // then
        assertThat( exception.getCause(), instanceOf( IllegalStateException.class ) );
        assertThat( exception.getCause().getMessage(), containsString( "Recovery is required" ) );
    }

    @Test
    void shouldFailToBootstrapWithUnrecoveredDatabase() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .needToRecover()
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer, pageCache,
                fileSystem, assertableLogProvider, storageEngineFactory, defaultConfig );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( membership, storeId ) );
        assertThat( exception.getCause(), instanceOf( IllegalStateException.class ) );
        assertableLogProvider.assertAtLeastOnce( inLog( RaftBootstrapper.class ).error( exception.getCause().getMessage() ) );
    }

    @Test
    void shouldFailToBootstrapWithUnrecoveredDatabaseWithCustomTransactionLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        File customTransactionLogsRootDirectory = testDirectory.directory( "custom-tx-logs-location" );
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .transactionLogsRootDirectory( customTransactionLogsRootDirectory )
                .needToRecover()
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, neo4jHome.toPath() )
                .set( transaction_logs_root_path, customTransactionLogsRootDirectory.toPath().toAbsolutePath() )
                .build();

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, databaseInitializer,
                pageCache, fileSystem, assertableLogProvider, storageEngineFactory, config );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( membership, storeId ) );
        assertableLogProvider.assertAtLeastOnce( inLog( RaftBootstrapper.class ).error( exception.getCause().getMessage() ) );
    }

    private void verifySnapshot( CoreSnapshot snapshot, Set<MemberId> expectedMembership, Config activeDatabaseConfig ) throws IOException
    {
        assertNotNull( snapshot );
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( expectedMembership, snapshot.get( CoreStateFiles.RAFT_CORE_STATE ).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateFiles.SESSION_TRACKER ) );

        for ( Map.Entry<DatabaseId,ClusteredDatabaseContext> databaseEntry : databaseManager.registeredDatabases().entrySet() )
        {
            verifyDatabaseSpecificState( snapshot::get );
            if ( databaseEntry.getKey().isSystemDatabase() )
            {
                verifyDatabase( databaseEntry.getValue().databaseLayout(), pageCache, Config.defaults() );
            }
            else
            {
                verifyDatabase( databaseEntry.getValue().databaseLayout(), pageCache, activeDatabaseConfig );
            }
        }
    }

    private void verifyDatabaseSpecificState( Function<CoreStateFiles<?>,?> databaseSpecific )
    {
        ReplicatedBarrierTokenState barrierTokenState = (ReplicatedBarrierTokenState) databaseSpecific.apply( CoreStateFiles.BARRIER_TOKEN );

        assertEquals( INITIAL_BARRIER_TOKEN, barrierTokenState );
    }

    private void verifyDatabase( DatabaseLayout databaseLayout, PageCache pageCache, Config config ) throws IOException
    {
        ReadOnlyTransactionStore transactionStore = new ReadOnlyTransactionStore( pageCache, fileSystem,
                databaseLayout, config, monitors );

        LastCommittedIndexFinder lastCommittedIndexFinder = new LastCommittedIndexFinder(
                new ReadOnlyTransactionIdStore( fileSystem, pageCache, databaseLayout ),
                transactionStore, logProvider );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private LogFiles buildLogFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem )
                .withConfig( defaultConfig )
                .build();
    }

    private static MemberId randomMember()
    {
        return new MemberId( randomUUID() );
    }
}
