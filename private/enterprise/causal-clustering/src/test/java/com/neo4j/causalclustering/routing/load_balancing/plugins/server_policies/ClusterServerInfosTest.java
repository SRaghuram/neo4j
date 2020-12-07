/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterServerInfosTest
{
    @Test
    void shouldSplitCoresCorrectlyWithNoLeader()
    {
        var cores = new ClusterServerInfos.ServerInfos( newServerInfosMap() );
        var readReplica = new ClusterServerInfos.ServerInfos( newServerInfosMap() );
        ServerId leader = null;
        var clusterServerInfos = new ClusterServerInfos( cores, readReplica, leader );

        assertThat( clusterServerInfos.cores().allServers() ).containsExactlyElementsOf( cores.allServers() );
        assertThat( clusterServerInfos.readReplicas().allServers() ).containsExactlyElementsOf( readReplica.allServers() );
        assertThat( clusterServerInfos.followers().allServers() ).containsExactlyElementsOf( cores.allServers() );
        assertThat( clusterServerInfos.leader().allServers() ).isEmpty();
    }

    @Test
    void shouldSplitCoresCorrectlyWithLeader()
    {
        var coreMap = newServerInfosMap();
        var cores = new ClusterServerInfos.ServerInfos( coreMap );
        var readReplica = new ClusterServerInfos.ServerInfos( newServerInfosMap() );
        ServerInfo leader = coreMap.keySet().iterator().next();
        var clusterServerInfos = new ClusterServerInfos( cores, readReplica, leader.serverId() );

        assertThat( clusterServerInfos.cores().allServers() ).containsExactlyElementsOf( cores.allServers() );
        assertThat( clusterServerInfos.readReplicas().allServers() ).containsExactlyElementsOf( readReplica.allServers() );
        assertThat( clusterServerInfos.followers().allServers() ).doesNotContain( leader );
        assertThat( clusterServerInfos.leader().allServers() ).containsExactly( leader );
    }

    @Test
    void shouldReflectOnlineState()
    {
        // given
        var serverInfoMap = new HashMap<ServerInfo,EnterpriseOperatorState>();
        var started = newServerInfo();
        serverInfoMap.put( started, EnterpriseOperatorState.STARTED );
        var stopped = newServerInfo();
        serverInfoMap.put( stopped, EnterpriseOperatorState.STOPPED );
        var serverInfos = new ClusterServerInfos.ServerInfos( serverInfoMap );

        // then
        assertThat( serverInfos.allServers() ).containsExactlyInAnyOrder( started, stopped );
        assertThat( serverInfos.onlineServers() ).containsExactly( started );
    }

    @Test
    void onlyStartedIsConsideredOnline()
    {
        var serverInfosMap = newServerInfosMap();
        var onlineServers = serverInfosMap.entrySet()
                .stream()
                .filter( e -> e.getValue() == EnterpriseOperatorState.STARTED )
                .map( Map.Entry::getKey )
                .toArray( ServerInfo[]::new );

        assertThat( new ClusterServerInfos.ServerInfos( serverInfosMap ).onlineServers() ).containsExactlyInAnyOrder( onlineServers );
    }

    static HashMap<ServerInfo,EnterpriseOperatorState> newServerInfosMap()
    {
        var serverInfoMap = new HashMap<ServerInfo,EnterpriseOperatorState>();
        for ( EnterpriseOperatorState value : EnterpriseOperatorState.values() )
        {
            serverInfoMap.put( newServerInfo(), value );
        }
        return serverInfoMap;
    }

    private static ServerInfo newServerInfo()
    {
        return new ServerInfo( new SocketAddress( 0 ), new ServerId( UUID.randomUUID() ), Set.of() );
    }
}
