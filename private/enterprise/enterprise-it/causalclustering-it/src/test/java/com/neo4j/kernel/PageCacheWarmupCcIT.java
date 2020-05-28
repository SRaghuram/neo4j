/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.upstream.strategies.LeaderOnlyStrategy;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitorAdapter;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ClusterExtension
class PageCacheWarmupCcIT extends PageCacheWarmupTestSupport
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private CoreClusterMember leader;

    @BeforeAll
    void beforeAll() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
                .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, TRUE )
                .withSharedCoreParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY )
                .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, id -> id == 0 ? FALSE : TRUE )
                .withSharedReadReplicaParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
                .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, TRUE )
                .withSharedReadReplicaParam( CausalClusteringSettings.pull_interval, "100ms" )
                .withSharedReadReplicaParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    private long warmUpCluster() throws Exception
    {
        leader = cluster.awaitLeader(); // Make sure we have a cluster leader.
        cluster.coreTx( ( db, tx ) ->
        {
            // Create some test data to touch a bunch of pages.
            createTestData( tx );
            tx.commit();
        } );
        AtomicLong pagesInMemory = new AtomicLong();
        cluster.coreTx( ( db, tx ) ->
        {
            // Wait for an initial profile on the leader. This profile might have raced with the 'createTestData'
            // transaction above, so it might be incomplete.
            waitForCacheProfile( leader.monitors() );
            // Now we can wait for a clean profile on the leader, and note the count for verifying later.
            pagesInMemory.set( waitForCacheProfile( leader.monitors() ) );
        } );
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            waitForCacheProfile( member.monitors() );
        }
        return pagesInMemory.get();
    }

    private static void verifyWarmupHappensAfterStoreCopy( ClusterMember member, long pagesInMemory )
    {
        AtomicLong pagesLoadedInWarmup = new AtomicLong();
        BinaryLatch warmupLatch = injectWarmupLatch( member, pagesLoadedInWarmup );
        member.start();
        warmupLatch.await();
        // Check that we warmup up all right:
        // we need + 1 here because of count store file that was touched and rotated on recovery
        assertThat( pagesLoadedInWarmup.get() + 1, greaterThanOrEqualTo( pagesInMemory ) );
    }

    private static BinaryLatch injectWarmupLatch( ClusterMember member, AtomicLong pagesLoadedInWarmup )
    {
        BinaryLatch warmupLatch = new BinaryLatch();
        Monitors monitors = member.monitors();
        monitors.addMonitorListener( new PageCacheWarmerMonitorAdapter()
        {
            @Override
            public void warmupCompleted( NamedDatabaseId namedDatabaseId, long pagesLoaded )
            {
                if ( DEFAULT_DATABASE_NAME.equals( namedDatabaseId.name() ) )
                {
                    pagesLoadedInWarmup.set( pagesLoaded );
                    warmupLatch.release();
                }
            }
        } );
        return warmupLatch;
    }

    @Test
    void cacheProfilesMustBeIncludedInStoreCopyToCore() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        CoreClusterMember member = cluster.newCoreMember();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }

    @Test
    void cacheProfilesMustBeIncludedInStoreCopyToReadReplica() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        ReadReplica member = cluster.newReadReplica();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }
}
