/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.IdFilesDeleter;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.common.StubLocalDatabaseService;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helpers.ClassicNeo4jDatabase;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.common.Service;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.internal.recordstorage.ReadOnlyTransactionIdStore;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState.INITIAL_LOCK_TOKEN;
import static java.lang.Integer.parseInt;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.helpers.collection.Iterators.asSet;

@PageCacheExtension
class CoreBootstrapperIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private final StubLocalDatabaseService databaseService = new StubLocalDatabaseService();

    private final Function<String,DatabaseInitializer> databaseInitializers = databaseName -> null;
    private final Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final Monitors monitors = new Monitors();

    private TemporaryDatabase.Factory temporaryDatabaseFactory;

    private File neo4jHome;
    private File dataDirectory;
    private File storeDirectory; // "databases"
    private File txLogsDirectory;
    private Config defaultConfig;
    private StorageEngineFactory storageEngineFactory;

    @BeforeEach
    void setup()
    {
        this.temporaryDatabaseFactory = new TemporaryDatabase.Factory( pageCache );
        this.neo4jHome = testDirectory.directory();
        this.dataDirectory = new File( neo4jHome, DEFAULT_DATA_DIR_NAME );
        this.storeDirectory = new File( dataDirectory, DEFAULT_DATABASES_ROOT_DIR_NAME );
        this.txLogsDirectory = new File( dataDirectory, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        this.defaultConfig = Config.builder().withHome( neo4jHome ).build();
        this.storageEngineFactory = StorageEngineFactory.selectStorageEngine( Service.loadAll( StorageEngineFactory.class ) );
    }

    @Test
    void shouldBootstrapWhenNoDirectoryExists() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory, () -> of( txLogsDirectory ), DEFAULT_DATABASE_NAME );
        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( databaseLayout )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, defaultConfig, logProvider, pageCache, storageEngineFactory );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, defaultConfig, 0 );
    }

    @Test
    void shouldBootstrapWhenEmptyDirectoryExists() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory, () -> of( txLogsDirectory ), DEFAULT_DATABASE_NAME );
        fileSystem.mkdirs( databaseLayout.databaseDirectory() );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( databaseLayout )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, defaultConfig, logProvider, pageCache, storageEngineFactory );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, defaultConfig, 0 );
    }

    @Test
    void shouldBootstrapFromSeed() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .build();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( database.layout() )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, defaultConfig, logProvider, pageCache, storageEngineFactory );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, defaultConfig, nodeCount );
    }

    @Test
    void shouldBootstrapWithCustomTransactionLogsLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        File customTransactionLogsRootDirectory = testDirectory.directory( "custom-tx-logs-location" );
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .transactionLogsRootDirectory( customTransactionLogsRootDirectory )
                .build();

        Config config = Config.builder().withHome( neo4jHome )
                              .withSetting( transaction_logs_root_path, customTransactionLogsRootDirectory.getAbsolutePath() )
                              .build();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( database.layout() )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, config, logProvider, pageCache, storageEngineFactory );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, config, nodeCount );
    }

    @Test
    void shouldBootstrapWithDeletedIdFiles() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .build();

        IdFilesDeleter.deleteIdFiles( database.layout(), fileSystem );

        databaseService.givenDatabaseWithConfig()
                       .withDatabaseName( DEFAULT_DATABASE_NAME )
                       .withDatabaseLayout( database.layout() )
                       .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, defaultConfig, logProvider, pageCache, storageEngineFactory );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, defaultConfig, nodeCount );
    }

    @Test
    void shouldFailToBootstrapWithUnrecoveredDatabase() throws Exception
    {
        // given
        int nodeCount = 100;
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .needToRecover()
                .build();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( database.layout() )
                .register();

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers, fileSystem,
                defaultConfig, assertableLogProvider, pageCache, storageEngineFactory );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> bootstrapper.bootstrap( membership ) );
        assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( exception.getMessage() ) );
    }

    @Test
    void shouldFailToBootstrapWithUnrecoveredDatabaseWithCustomTransactionLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        File customTransactionLogsRootDirectory = testDirectory.directory( "custom-tx-logs-location" );
        ClassicNeo4jDatabase database = ClassicNeo4jDatabase
                .builder( dataDirectory, fileSystem )
                .databaseName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .transactionLogsRootDirectory( customTransactionLogsRootDirectory )
                .needToRecover()
                .build();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( database.layout() )
                .register();

        Config config = Config
                .builder()
                .withHome( neo4jHome )
                .withSetting( transaction_logs_root_path, customTransactionLogsRootDirectory.getAbsolutePath() )
                .build();

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, config, assertableLogProvider, pageCache, storageEngineFactory );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        Exception exception = assertThrows( Exception.class, () -> bootstrapper.bootstrap( membership ) );
        assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( exception.getMessage() ) );
    }

    private void verifySnapshot( CoreSnapshot snapshot, Set<MemberId> expectedMembership, Config activeDatabaseConfig, int nodeCount ) throws IOException
    {
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( expectedMembership, snapshot.get( CoreStateFiles.RAFT_CORE_STATE ).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateFiles.SESSION_TRACKER ) );

        for ( Map.Entry<String,LocalDatabase> databaseEntry : databaseService.registeredDatabases().entrySet() )
        {
            verifyDatabaseSpecificState( type -> snapshot.get( databaseEntry.getKey(), type ), nodeCount );
            if ( databaseEntry.getKey().equals( SYSTEM_DATABASE_NAME ) )
            {
                verifyDatabase( databaseEntry.getValue().databaseLayout(), pageCache, Config.defaults() );
            }
            else
            {
                verifyDatabase( databaseEntry.getValue().databaseLayout(), pageCache, activeDatabaseConfig );
            }
        }
    }

    private void verifyDatabaseSpecificState( Function<CoreStateFiles<?>,?> databaseSpecific, int nodeCount )
    {
        ReplicatedLockTokenState lockTokenState = (ReplicatedLockTokenState) databaseSpecific.apply( CoreStateFiles.LOCK_TOKEN );
        IdAllocationState idAllocationState =  (IdAllocationState) databaseSpecific.apply( CoreStateFiles.ID_ALLOCATION );

        assertEquals( INITIAL_LOCK_TOKEN, lockTokenState );

        assertThat( idAllocationState.firstUnallocated( IdType.NODE ),
                allOf( greaterThanOrEqualTo( (long) nodeCount ), lessThanOrEqualTo( (long) nodeCount + recordIdBatchSize() ) ) );
    }

    private void verifyDatabase( DatabaseLayout databaseLayout, PageCache pageCache, Config config ) throws IOException
    {
        ReadOnlyTransactionStore transactionStore = new ReadOnlyTransactionStore( pageCache, fileSystem,
                databaseLayout, config, monitors );

        LastCommittedIndexFinder lastCommittedIndexFinder = new LastCommittedIndexFinder(
                new ReadOnlyTransactionIdStore( pageCache, databaseLayout ),
                transactionStore, logProvider );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private int recordIdBatchSize()
    {
        return parseInt( record_id_batch_size.getDefaultValue() );
    }

    private static MemberId randomMember()
    {
        return new MemberId( randomUUID() );
    }
}
