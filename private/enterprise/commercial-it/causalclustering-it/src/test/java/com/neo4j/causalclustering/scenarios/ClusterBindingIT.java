/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.upstream.TestStoreId.assertAllStoresHaveTheSameStoreId;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

public class ClusterBindingIT
{
    private final ClusterRule clusterRule = new ClusterRule()
                        .withNumberOfCoreMembers( 3 )
                        .withNumberOfReadReplicas( 0 )
                        .withSharedCoreParam( CausalClusteringSettings.raft_log_pruning_strategy, "3 entries" )
                        .withSharedCoreParam( CausalClusteringSettings.raft_log_rotation_size, "1K" );
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );

    private Cluster cluster;
    private FileSystemAbstraction fs;

    @Before
    public void setup() throws Exception
    {
        fs = fileSystemRule.get();
        cluster = clusterRule.startCluster();
        DataCreator.createSchema( cluster );
    }

    @Test
    public void allServersShouldHaveTheSameStoreId() throws Throwable
    {
        // WHEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        List<DatabaseLayout> databaseLayouts = databaseLayouts( cluster.coreMembers() );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    public void whenWeRestartTheClusterAllServersShouldStillHaveTheSameStoreId() throws Throwable
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        cluster.shutdown();
        // WHEN
        cluster.start();

        List<DatabaseLayout> databaseLayouts = databaseLayouts( cluster.coreMembers() );

        DataCreator.createDataInOneTransaction( cluster, 1 );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    @Ignore( "Fix this test by having the bootstrapper augment his store and bind it using store-id on disk." )
    public void shouldNotJoinClusterIfHasDataWithDifferentStoreId() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        DatabaseLayout databaseLayout = cluster.getCoreMemberById( 0 ).databaseLayout();

        cluster.removeCoreMemberWithServerId( 0 );
        changeStoreId( databaseLayout );

        // WHEN
        try
        {
            cluster.addCoreMemberWithId( 0 ).start();
            fail( "Should not have joined the cluster" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), instanceOf( LifecycleException.class ) );
        }
    }

    @Test
    public void laggingFollowerShouldDownloadSnapshot() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        //TODO: Work out if/why this won't potentially remove a leader?
        cluster.removeCoreMemberWithServerId( 0 );

        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.raftLogPruner().prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 0 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 3, cluster.healthyCoreMembers().size() );

        List<DatabaseLayout> databaseLayouts = databaseLayouts( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    @Test
    public void badFollowerShouldNotJoinCluster() throws Exception
    {
        // GIVEN
        DataCreator.createDataInOneTransaction( cluster, 1 );

        CoreClusterMember coreMember = cluster.getCoreMemberById( 0 );
        cluster.removeCoreMemberWithServerId( 0 );
        DatabaseId systemDatabase = new DatabaseId( SYSTEM_DATABASE_NAME );
        changeRaftId( coreMember, systemDatabase );

        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.raftLogPruner().prune();
        }

        // WHEN
        try
        {
            cluster.addCoreMemberWithId( 0 ).start();
            fail( "Should not have joined the cluster" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), instanceOf( UnableToStartDatabaseException.class ) );
        }
    }

    @Test
    public void aNewServerShouldJoinTheClusterByDownloadingASnapshot() throws Exception
    {
        // GIVEN
        DataCreator.createDataInMultipleTransactions( cluster, 100 );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.raftLogPruner().prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 4 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 4, cluster.healthyCoreMembers().size() );

        List<DatabaseLayout> databaseLayouts = databaseLayouts( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( databaseLayouts, fs );
    }

    private static List<DatabaseLayout> databaseLayouts( Collection<CoreClusterMember> dbs )
    {
        return dbs.stream().map( CoreClusterMember::databaseLayout ).collect( Collectors.toList() );
    }

    private void changeRaftId( CoreClusterMember coreMember, DatabaseId databaseId ) throws IOException
    {
        ClusterStateLayout layout = coreMember.clusterStateLayout();
        CoreStateStorageFactory storageFactory = new CoreStateStorageFactory( fs, layout, NullLogProvider.getInstance(), coreMember.config() );
        SimpleStorage<RaftId> raftIdStorage = storageFactory.createRaftIdStorage( databaseId, nullDatabaseLogProvider() );
        raftIdStorage.writeState( new RaftId( UUID.randomUUID() ) );
    }

    private void changeStoreId( DatabaseLayout databaseLayout ) throws Exception
    {
        File neoStoreFile = databaseLayout.metadataStore();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, RANDOM_NUMBER, System.currentTimeMillis() );
        }
    }
}
