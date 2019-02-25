/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.causalclustering.upstream.strategies.LeaderOnlyStrategy;
import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitorAdapter;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class PageCacheWarmupCcIT extends PageCacheWarmupTestSupport
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedCoreParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedCoreParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY )
            .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, id -> id == 0 ? "false" : "true" )
            .withSharedReadReplicaParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedReadReplicaParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedReadReplicaParam( CausalClusteringSettings.pull_interval, "100ms" )
            .withSharedReadReplicaParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY );

    private Cluster<?> cluster;
    private CoreClusterMember leader;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    private long warmUpCluster() throws Exception
    {
        leader = cluster.awaitLeader(); // Make sure we have a cluster leader.
        cluster.coreTx( ( db, tx ) ->
        {
            // Create some test data to touch a bunch of pages.
            createTestData( db );
            tx.success();
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
            public void warmupCompleted( long pagesLoaded )
            {
                pagesLoadedInWarmup.set( pagesLoaded );
                warmupLatch.release();
            }
        } );
        return warmupLatch;
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToCore() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        CoreClusterMember member = cluster.newCoreMember();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToReadReplica() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        ReadReplica member = cluster.newReadReplica();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }
}
