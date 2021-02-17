/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_enabled;

@ClusterExtension
public class BoltServerIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void setupClass() throws ExecutionException, InterruptedException, TimeoutException
    {
        var sharedParams = Map.of( routing_enabled.name(), Boolean.TRUE.toString() );
        var clusterConfig = ClusterConfig
                .clusterConfig()
                .withSharedCoreParams( sharedParams )
                .withSharedReadReplicaParams( sharedParams );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        cluster.awaitLeader();
    }

    @Test
    void readReplicasShouldNotListenOnIntraClusterBoltAddress()
    {
        Set<ReadReplica> replicas = cluster.readReplicas();
        assertTrue( replicas.size() > 0 );
        replicas.forEach(
                replica ->
                {
                    SocketAddress addr = replica.config().get( GraphDatabaseSettings.routing_advertised_address );
                    try ( Socket ignored = new Socket( addr.getHostname(), addr.getPort() ) )
                    {
                        fail( "The read replica should not listed on intra cluster bolt address" );
                    }
                    catch ( ConnectException e )
                    {
                        // expected
                    }
                    catch ( Exception e )
                    {
                        fail( "Unexpected exception", e );
                    }
                } );
    }

    @AfterAll
    void shutdownClass()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }
}
