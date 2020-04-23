/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.upstream.TestStoreId.assertAllStoresHaveTheSameStoreId;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( DefaultFileSystemExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
class ClusterBindingIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( CausalClusteringSettings.raft_log_pruning_strategy, "3 entries" )
                .withSharedCoreParam( CausalClusteringSettings.raft_log_rotation_size, "1K" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        DataCreator.createSchema( cluster );
    }

    @Test
    void allServersShouldHaveTheSameStoreId() throws Throwable
    {
        // WHEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        var databaseLayouts = databaseLayouts( cluster.coreMembers() );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    void whenWeRestartTheClusterAllServersShouldStillHaveTheSameStoreId() throws Throwable
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        cluster.shutdown();
        // WHEN
        cluster.start();

        var databaseLayouts = databaseLayouts( cluster.coreMembers() );

        DataCreator.createDataInOneTransaction( cluster, 1 );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    @Disabled( "Fix this test by having the bootstrapper augment his store and bind it using store-id on disk." )
    void shouldNotJoinClusterIfHasDataWithDifferentStoreId() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        var databaseLayout = cluster.getCoreMemberById( 0 ).databaseLayout();

        cluster.removeCoreMemberWithServerId( 0 );
        changeStoreId( databaseLayout );

        // WHEN / THEN
        var error = assertThrows( RuntimeException.class, () -> cluster.addCoreMemberWithId( 0 ).start(), "Should not have joined the cluster" );
        assertThat( error.getCause(), instanceOf( LifecycleException.class ) );
    }

    @Test
    void laggingFollowerShouldDownloadSnapshot() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        //TODO: Work out if/why this won't potentially remove a leader?
        cluster.removeCoreMemberWithServerId( 0 );

        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( var db : cluster.coreMembers() )
        {
            db.resolveDependency( DEFAULT_DATABASE_NAME, RaftLogPruner.class ).prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 0 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 3, cluster.healthyCoreMembers().size() );

        var databaseLayouts = databaseLayouts( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    @Disabled( "Fix is already in 3.6 and will be eventually forward merged" )
    void badFollowerShouldNotJoinCluster() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        var coreMember = cluster.getCoreMemberById( 0 );
        cluster.removeCoreMemberWithServerId( 0 );
        changeRaftId( coreMember, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( var db : cluster.coreMembers() )
        {
            db.resolveDependency( DEFAULT_DATABASE_NAME, RaftLogPruner.class ).prune();
        }

        // WHEN / THEN
        var error = assertThrows( RuntimeException.class, () -> cluster.addCoreMemberWithId( 0 ).start(), "Should not have joined the cluster" );
        assertThat( error.getCause(), instanceOf( UnableToStartDatabaseException.class ) );
    }

    @Test
    void aNewServerShouldJoinTheClusterByDownloadingASnapshot() throws Exception
    {
        // GIVEN
        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( var db : cluster.coreMembers() )
        {
            db.resolveDependency( DEFAULT_DATABASE_NAME, RaftLogPruner.class ).prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 4 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 4, cluster.healthyCoreMembers().size() );

        var databaseLayouts = databaseLayouts( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    private static List<DatabaseLayout> databaseLayouts( Collection<CoreClusterMember> dbs )
    {
        return dbs.stream().map( CoreClusterMember::databaseLayout ).collect( Collectors.toList() );
    }

    private void changeRaftId( CoreClusterMember coreMember, String databaseName ) throws IOException
    {
        var layout = coreMember.clusterStateLayout();
        var storageFactory = new ClusterStateStorageFactory( fs, layout, NullLogProvider.getInstance(), coreMember.config(), INSTANCE );
        var raftIdStorage = storageFactory.createRaftIdStorage( databaseName, nullDatabaseLogProvider() );
        raftIdStorage.writeState( RaftIdFactory.random() );
    }

    private void changeStoreId( DatabaseLayout databaseLayout ) throws Exception
    {
        var neoStoreFile = databaseLayout.metadataStore();
        try ( var jobScheduler = new ThreadPoolJobScheduler();
              var pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler, PageCacheTracer.NULL ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, RANDOM_NUMBER, System.currentTimeMillis(), NULL );
        }
    }
}
