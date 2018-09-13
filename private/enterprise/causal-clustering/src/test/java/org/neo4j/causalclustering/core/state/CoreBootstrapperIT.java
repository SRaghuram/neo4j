/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.helpers.ClassicNeo4jStore;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreBootstrapperIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final StubLocalDatabaseService databaseService = new StubLocalDatabaseService();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule ).around( testDirectory );

    @Test
    public void shouldSetAllCoreState() throws Exception
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        File classicNeo4jStore = ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                .amountOfNodes( nodeCount )
                .build()
                .getStoreDir();
        DatabaseLayout databaseLayout = DatabaseLayout.of( classicNeo4jStore );

        //TODO: Consider providing a data source manager as well - when we have NeoStoreDataSourceRule playing nice with cluster
        databaseService.givenDatabaseWithConfig()
                .withDatabaseLayout( databaseLayout )
                .withMonitors( new Monitors() )
                .register();

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, fileSystem, Config.defaults(),
                NullLogProvider.getInstance(), pageCache, new Monitors() );
        bootstrapAndVerify( nodeCount, fileSystem, databaseLayout, pageCache, Config.defaults(), bootstrapper, DEFAULT_DATABASE_NAME );
    }

    @Test
    public void setAllCoreStateOnDatabaseWithCustomLogFilesLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        String customTransactionLogsLocation = "transaction-logs";
        File classicNeo4jStore = ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .build()
                .getStoreDir();
        DatabaseLayout databaseLayout = DatabaseLayout.of( classicNeo4jStore );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseLayout( databaseLayout )
                .withMonitors( new Monitors() )
                .register();

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location,
                customTransactionLogsLocation );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, fileSystem, config, NullLogProvider.getInstance(), pageCache, new Monitors() );

        bootstrapAndVerify( nodeCount, fileSystem, databaseLayout, pageCache, config, bootstrapper, DEFAULT_DATABASE_NAME );
    }

    @Test
    public void shouldFailToBootstrapIfClusterIsInNeedOfRecovery() throws IOException
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        File storeInNeedOfRecovery =
                ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem )
                        .amountOfNodes( nodeCount )
                        .needToRecover()
                        .build()
                        .getStoreDir();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider(  );
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeInNeedOfRecovery );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseLayout( databaseLayout )
                .withMonitors( new Monitors() )
                .register();

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.defaults();
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, fileSystem, config, assertableLogProvider, pageCache, new Monitors() );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( Exception e )
        {
            String errorMessage = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            assertEquals( e.getMessage(), errorMessage );
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( errorMessage) );
        }
    }

    @Test
    public void shouldFailToBootstrapIfClusterIsInNeedOfRecoveryWithCustomLogicalLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        String customTransactionLogsLocation = "transaction-logs";
        File storeInNeedOfRecovery = ClassicNeo4jStore
                .builder( testDirectory.directory(), fileSystem )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .needToRecover()
                .build()
                .getStoreDir();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider(  );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        DatabaseLayout databaseLayout = DatabaseLayout.of( storeInNeedOfRecovery );

        databaseService.givenDatabaseWithConfig()
                .withDatabaseLayout( databaseLayout )
                .withMonitors( new Monitors() )
                .register();

        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogsLocation );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( databaseService, fileSystem, config, assertableLogProvider, pageCache, new Monitors() );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( Exception e )
        {
            String errorMessage = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            assertEquals( e.getMessage(), errorMessage );
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( errorMessage) );
        }
    }

    private static void bootstrapAndVerify( long nodeCount, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache, Config config,
            CoreBootstrapper bootstrapper, String databaseName ) throws Exception
    {
        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        int recordIdBatchSize = parseInt( record_id_batch_size.getDefaultValue() );
        assertThat( snapshot.get( databaseName, CoreStateFiles.ID_ALLOCATION ).firstUnallocated( IdType.NODE ),
                allOf( greaterThanOrEqualTo( nodeCount ), lessThanOrEqualTo( nodeCount + recordIdBatchSize ) ) );

        /* Bootstrapped state is created in RAFT land at index -1 and term -1. */
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Lock is initially not taken. */
        assertEquals( new ReplicatedLockTokenState(), snapshot.get( databaseName, CoreStateFiles.LOCK_TOKEN ) );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( membership, snapshot.get( CoreStateFiles.RAFT_CORE_STATE ).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateFiles.SESSION_TRACKER ) );

        ReadOnlyTransactionStore transactionStore = new ReadOnlyTransactionStore( pageCache, fileSystem,
                databaseLayout, config, new Monitors() );
        LastCommittedIndexFinder lastCommittedIndexFinder = new LastCommittedIndexFinder(
                new ReadOnlyTransactionIdStore( pageCache, databaseLayout ),
                transactionStore, NullLogProvider.getInstance() );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private static MemberId randomMember()
    {
        return new MemberId( randomUUID() );
    }
}
