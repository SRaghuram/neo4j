/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;

public class SlaveOnlyClusterIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withInstanceSetting( HaSettings.slave_only,
                    value -> value == 1 || value == 2 ? Settings.TRUE : Settings.FALSE );

    private static final String PROPERTY = "foo";
    private static final String VALUE = "bar";

    @Test
    public void testMasterElectionAfterMasterRecoversInSlaveOnlyCluster() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        assertThat( cluster.getServerId( cluster.getMaster() ), equalTo( new InstanceId( 3 ) ) );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        CountDownLatch masterFailedLatch = createMasterFailLatch( cluster );
        RepairKit repairKit = cluster.fail( master );
        try
        {
            assertTrue( masterFailedLatch.await( 60, TimeUnit.SECONDS ) );
        }
        finally
        {
            repairKit.repair();
        }

        cluster.await( allSeesAllAsAvailable() );
        long nodeId = createNodeWithPropertyOn( cluster.getAnySlave(), PROPERTY, VALUE );

        try ( Transaction ignore = master.beginTx() )
        {
            assertThat( master.getNodeById( nodeId ).getProperty( PROPERTY ), equalTo( VALUE ) );
        }
    }

    private long createNodeWithPropertyOn( HighlyAvailableGraphDatabase db, String property, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( property, value );

            tx.success();

            return node.getId();
        }
    }

    private CountDownLatch createMasterFailLatch( ClusterManager.ManagedCluster cluster )
    {
        final CountDownLatch failedLatch = new CountDownLatch( 2 );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            if ( !db.isMaster() )
            {
                db.getDependencyResolver().resolveDependency( ClusterClient.class )
                        .addHeartbeatListener( new HeartbeatListener()
                        {
                            @Override
                            public void failed( InstanceId server )
                            {
                                failedLatch.countDown();
                            }

                            @Override
                            public void alive( InstanceId server )
                            {
                            }
                        } );
            }
        }
        return failedLatch;
    }
}
