/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.EnterpriseTemporaryDatabaseFactory;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.helpers.ClassicNeo4jDatabase;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.ReadOnlyTransactionIdStore;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState.INITIAL_LEASE_STATE;
import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_STORE_VERSION;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@PageCacheExtension
@Neo4jLayoutExtension
class RaftBootstrapperIT
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory testDirectory;
    @SuppressWarnings( "unused" )
    @Inject
    private PageCache pageCache;
    @SuppressWarnings( "unused" )
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @SuppressWarnings( "unused" )
    @Inject
    private DatabaseLayout databaseLayout;

    private static final NamedDatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private static final NamedDatabaseId SYSTEM_DATABASE_ID = new TestDatabaseIdRepository().getByName( SYSTEM_DATABASE_NAME ).orElseThrow();
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();

    private final Set<RaftMemberId> raftMembers = asSet( randomMember(), randomMember(), randomMember() );
    private final StoreId storeId = new StoreId( MetaDataStore.versionStringToLong( LATEST_STORE_VERSION ) );

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final Monitors monitors = new Monitors();

    private TemporaryDatabaseFactory temporaryDatabaseFactory;

    private Path neo4jHome;
    private Path dataDirectory;
    private Config defaultConfig;
    private StorageEngineFactory storageEngineFactory;
    private final BootstrapSaver bootstrapSaver = new BootstrapSaver( fileSystem, nullLogProvider() );
    private final PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();

    @BeforeEach
    void setup()
    {
        this.temporaryDatabaseFactory = new EnterpriseTemporaryDatabaseFactory( pageCache, fileSystem );
        this.neo4jHome = testDirectory.homePath();
        this.defaultConfig = Config.defaults( GraphDatabaseSettings.neo4j_home, neo4jHome );
        this.dataDirectory = defaultConfig.get( GraphDatabaseSettings.data_directory );
        this.storageEngineFactory = StorageEngineFactory.selectStorageEngine();
    }

    @Test
    void tracePageCacheAccessOnBootstrap() throws IOException
    {
        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );
        verifySnapshot( snapshot, raftMembers, defaultConfig );

        assertEquals( 21, pageCacheTracer.pins() );
        assertEquals( 21, pageCacheTracer.unpins() );
        assertEquals( 5, pageCacheTracer.hits() );
        assertEquals( 16, pageCacheTracer.faults() );
    }

    @Test
    void shouldBootstrapWhenNoDirectoryExists() throws Exception
    {
        // given
        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );

        // then
        verifySnapshot( snapshot, raftMembers, defaultConfig );
    }

    @Test
    void shouldBootstrapWhenEmptyDirectoryExists() throws Exception
    {
        // given
        fileSystem.mkdirs( databaseLayout.databaseDirectory() );
        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );

        // then
        verifySnapshot( snapshot, raftMembers, defaultConfig );
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

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );

        // then
        verifySnapshot( snapshot, raftMembers, defaultConfig );
        assertFalse( fileSystem.fileExists( new File( databaseLayout.databaseDirectory().toFile(), TEMP_BOOTSTRAP_DIRECTORY_NAME ).toPath() ) );
    }

    @Test
    void shouldBootstrapSystemFromSeed() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( SYSTEM_DATABASE_ID )
                .amountOfNodes( nodeCount )
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        DatabaseLayout databaseLayout = database.layout();
        LogFiles transactionLogs = buildLogFiles( databaseLayout );
        BootstrapContext bootstrapContext = new BootstrapContext( SYSTEM_DATABASE_ID, databaseLayout, storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );

        // then
        verifySnapshot( snapshot, raftMembers, defaultConfig );
        assertFalse( fileSystem.fileExists( new File( databaseLayout.databaseDirectory().toFile(), TEMP_BOOTSTRAP_DIRECTORY_NAME ).toPath() ) );
    }

    @Test
    void shouldBootstrapWithCustomTransactionLogsLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        Path customTransactionLogsRootDirectory = testDirectory.directoryPath( "custom-tx-logs-location" );
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseId( DATABASE_ID )
                .amountOfNodes( nodeCount )
                .transactionLogsRootDirectory( customTransactionLogsRootDirectory )
                .build();

        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, neo4jHome )
                .set( transaction_logs_root_path, customTransactionLogsRootDirectory.toAbsolutePath() )
                .build();

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, config, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( raftMembers, storeId );

        // then
        verifySnapshot( snapshot, raftMembers, config );
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

        for ( Path idFile : database.layout().idFiles() )
        {
            fileSystem.deleteFileOrThrow( idFile );
        }

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );
        LogFiles transactionLogs = buildLogFiles( database.layout() );
        BootstrapContext bootstrapContext = new BootstrapContext( DATABASE_ID, database.layout(), storeFiles, transactionLogs );

        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, logProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( raftMembers, storeId ) );

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
        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory, pageCache,
                fileSystem, assertableLogProvider, storageEngineFactory, defaultConfig, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        Set<RaftMemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( raftMembers, storeId ) );
        assertThat( exception.getCause(), instanceOf( IllegalStateException.class ) );
        assertThat( assertableLogProvider ).forClass( RaftBootstrapper.class ).forLevel( ERROR )
                .containsMessages( exception.getCause().getMessage() );
    }

    @Test
    void shouldFailToBootstrapWithUnrecoveredDatabaseWithCustomTransactionLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        Path customTransactionLogsRootDirectory = testDirectory.directoryPath( "custom-tx-logs-location" );
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
                .set( GraphDatabaseSettings.neo4j_home, neo4jHome )
                .set( transaction_logs_root_path, customTransactionLogsRootDirectory.toAbsolutePath() )
                .build();

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        RaftBootstrapper bootstrapper = new RaftBootstrapper( bootstrapContext, temporaryDatabaseFactory,
                pageCache, fileSystem, assertableLogProvider, storageEngineFactory, config, bootstrapSaver, pageCacheTracer, INSTANCE );

        // when
        Set<RaftMemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        BootstrapException exception = assertThrows( BootstrapException.class, () -> bootstrapper.bootstrap( raftMembers, storeId ) );
        assertThat( assertableLogProvider ).forClass( RaftBootstrapper.class ).forLevel( ERROR )
                .containsMessages( exception.getCause().getMessage() );
    }

    private void verifySnapshot( CoreSnapshot snapshot, Set<RaftMemberId> expectedMembership, Config activeDatabaseConfig ) throws IOException
    {
        assertNotNull( snapshot );
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( expectedMembership, snapshot.get( CoreStateFiles.RAFT_CORE_STATE ).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateFiles.SESSION_TRACKER ) );

        for ( Map.Entry<NamedDatabaseId,ClusteredDatabaseContext> databaseEntry : databaseManager.registeredDatabases().entrySet() )
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
        ReplicatedLeaseState leaseState = (ReplicatedLeaseState) databaseSpecific.apply( CoreStateFiles.LEASE );

        assertEquals( INITIAL_LEASE_STATE, leaseState );
    }

    private void verifyDatabase( DatabaseLayout databaseLayout, PageCache pageCache, Config config )
            throws IOException
    {
        CommandReaderFactory commandReaderFactory = StorageEngineFactory.selectStorageEngine().commandReaderFactory();
        ReadOnlyTransactionStore transactionStore =
                new ReadOnlyTransactionStore( pageCache, fileSystem, databaseLayout, config, monitors, commandReaderFactory );

        LastCommittedIndexFinder lastCommittedIndexFinder =
                new LastCommittedIndexFinder( new ReadOnlyTransactionIdStore( fileSystem, pageCache, databaseLayout, NULL ), transactionStore, logProvider );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private LogFiles buildLogFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem )
                .withConfig( defaultConfig )
                .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                .build();
    }

    private static RaftMemberId randomMember()
    {
        return IdFactory.randomRaftMemberId();
    }
}
