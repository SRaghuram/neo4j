/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.helper.TemporaryDatabase;
import org.neo4j.causalclustering.helpers.ClassicNeo4jStore;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredException;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState.INITIAL_LOCK_TOKEN;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreBootstrapperIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final Config activeDatabaseConfig = Config.defaults();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule ).around( testDirectory );

    private final FileSystemAbstraction fileSystem = fileSystemRule.get();
    private final PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

    private final StubLocalDatabaseService databaseService = new StubLocalDatabaseService();

    private final TemporaryDatabase.Factory temporaryDatabaseFactory = new TemporaryDatabase.Factory( pageCache );

    private final Function<String,DatabaseInitializer> databaseInitializers = databaseName -> null;
    private final Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final Monitors monitors = new Monitors();

    @Test
    public void shouldBootstrapWhenNoDirectoryExists() throws Exception
    {
        // given
        File notExistingDirectory = new File( testDirectory.directory(), DEFAULT_DATABASE_NAME );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( notExistingDirectory ) )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, activeDatabaseConfig, logProvider, pageCache );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, activeDatabaseConfig, 0 );
    }

    @Test
    public void shouldBootstrapWhenEmptyDirectoryExists() throws IOException, RecoveryRequiredException
    {
        File databaseDirectory = new File( testDirectory.directory(), DEFAULT_DATABASE_NAME );
        fileSystem.mkdir( databaseDirectory );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( databaseDirectory ) )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, activeDatabaseConfig, logProvider, pageCache );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, activeDatabaseConfig, 0 );
    }

    @Test
    public void shouldBootstrapFromSeed() throws Exception
    {
        // given
        int nodeCount = 100;
        File classicNeo4jStore = ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                .dbName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .build()
                .getStoreDir();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( classicNeo4jStore ) )
                .register();

        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, activeDatabaseConfig, logProvider, pageCache );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, activeDatabaseConfig, nodeCount );
    }

    @Test
    public void shouldBootstrapWithCustomTransationLogsLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        String customTransactionLogsLocation = "transaction-logs";
        File classicNeo4jStore = ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                .dbName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .build()
                .getStoreDir();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( classicNeo4jStore ) )
                .register();

        Config activeDatabaseConfig = Config.defaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogsLocation );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, activeDatabaseConfig, logProvider, pageCache );

        // when
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        verifySnapshot( snapshot, membership, activeDatabaseConfig, nodeCount );
    }

    @Test
    public void shouldFailToBootstrapWithUnrecoveredStore() throws IOException
    {
        // given
        int nodeCount = 100;
        File storeInNeedOfRecovery =
                ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                        .dbName( DEFAULT_DATABASE_NAME )
                        .amountOfNodes( nodeCount )
                        .needToRecover()
                        .build()
                        .getStoreDir();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( storeInNeedOfRecovery ) )
                .register();

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers, fileSystem,
                activeDatabaseConfig, assertableLogProvider, pageCache );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( RecoveryRequiredException e )
        {
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( e.getMessage() ) );
        }
    }

    @Test
    public void shouldFailToBootstrapWithUnrecoveredStoreWithCustomTransactionLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        String customTransactionLogsLocation = "transaction-logs";
        File storeInNeedOfRecovery = ClassicNeo4jStore
                .builder( testDirectory.directory(), fileSystem )
                .dbName( DEFAULT_DATABASE_NAME )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .needToRecover()
                .build()
                .getStoreDir();

        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withDatabaseLayout( DatabaseLayout.of( storeInNeedOfRecovery ) )
                .register();

        Config activeDatabaseConfig = Config.defaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogsLocation );

        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, temporaryDatabaseFactory, databaseInitializers,
                fileSystem, activeDatabaseConfig, assertableLogProvider, pageCache );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( Exception e )
        {
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( e.getMessage() ) );
        }
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
                verifyStore( databaseEntry.getValue().databaseLayout(), pageCache, Config.defaults() );
            }
            else
            {
                verifyStore( databaseEntry.getValue().databaseLayout(), pageCache, activeDatabaseConfig );
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

    private void verifyStore( DatabaseLayout databaseLayout, PageCache pageCache, Config config ) throws IOException
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
