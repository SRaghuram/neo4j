/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.read_replica.SpecificReplicaStrategy.upstreamFactory;
import static com.neo4j.configuration.CausalClusteringInternalSettings.inbound_connection_initialization_logging_enabled;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.logging.LogAssertions.assertThat;

@ClusterExtension
@TestInstance( PER_METHOD )
class NettyChannelIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @Test
    void logsShouldContainSocketImplementationClassInformation() throws Exception
    {
        // given
        cluster = startCluster( 3, 1 );
        CoreClusterMember coreToCheck = cluster.awaitLeader();
        ReadReplica readReplica = cluster.findAnyReadReplica();

        // A read replica that we force to pull from the original so the original logs socket server messages
        ReadReplica newReplica = cluster.addReadReplicaWithIndex( 1 );
        newReplica.config().set( CausalClusteringSettings.upstream_selection_strategy, List.of( "specific" ) );
        newReplica.config().set( CausalClusteringSettings.multi_dc_license, true );
        upstreamFactory.setCurrent( readReplica );

        Cluster.startMembers( newReplica );

        // then
        AssertableLogProvider coreLog = coreToCheck.getAssertableLogProvider();
        AssertableLogProvider readReplicaLog = readReplica.getAssertableLogProvider();

        var expectedSocketClass = KQueue.isAvailable() ? KQueueSocketChannel.class
                                                       : Epoll.isAvailable() ? EpollSocketChannel.class
                                                                             : NioSocketChannel.class;

        assertLogsContainSocketImplementationDetails( coreLog, expectedSocketClass );
        assertLogsContainSocketImplementationDetails( readReplicaLog, expectedSocketClass );
    }

    private void assertLogsContainSocketImplementationDetails( AssertableLogProvider log,
                                                               Class<? extends io.netty.channel.AbstractChannel> expectedSocketClass )
    {
        var clientHandshakeLogs = log.serialize().lines()
                                     .filter( l -> l.contains( "Initiating handshake on channel" ) )
                                     .collect( Collectors.toList() );

        assertThat( clientHandshakeLogs ).isNotEmpty();
        assertThat( clientHandshakeLogs ).allSatisfy( l -> assertThat( l ).contains( String.format( "[%s]", expectedSocketClass ) ) )
                                         .allSatisfy( l -> assertThat( l ).contains( HandshakeClientInitializer.class.getName() ) );

        var severHandshakeLogs = log.serialize().lines()
                                    .filter( l -> l.contains( "Installing handshake server on channel" ) )
                                    .collect( Collectors.toList() );

        assertThat( severHandshakeLogs ).isNotEmpty();
        assertThat( severHandshakeLogs ).allSatisfy( l -> assertThat( l ).contains( String.format( "[%s]", expectedSocketClass ) ) )
                                        .allSatisfy( l -> assertThat( l ).contains( HandshakeServerInitializer.class.getName() ) );
    }

    private Cluster startCluster( int coreCount, int readReplicaCount ) throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( coreCount )
                .withSharedPrimaryParam( inbound_connection_initialization_logging_enabled, "true" )
                .withNumberOfReadReplicas( readReplicaCount )
                .withSharedReadReplicaParam( inbound_connection_initialization_logging_enabled, "true" )
                .withLogProvider( AssertableLogProvider::new );
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        return cluster;
    }
}
