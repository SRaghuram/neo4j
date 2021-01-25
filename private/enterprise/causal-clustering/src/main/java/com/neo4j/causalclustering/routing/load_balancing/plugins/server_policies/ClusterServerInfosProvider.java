/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class ClusterServerInfosProvider implements Function<NamedDatabaseId,ClusterServerInfos>
{
    private final TopologyService topologyService;
    private final LeaderService leaderService;

    public ClusterServerInfosProvider( TopologyService topologyService, LeaderService leaderService )
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
    }

    @Override
    public ClusterServerInfos apply( NamedDatabaseId namedDatabaseId )
    {
        return topologySnapshot( namedDatabaseId, topologyService, leaderService );
    }

    /**
     * {@link LeaderService} is more likely to be up to date and is therefore used to determine leader.
     */
    private static ClusterServerInfos topologySnapshot( NamedDatabaseId namedDatabaseId, TopologyService topologyService, LeaderService leaderService )
    {
        var coreTopology = serverInfo( topologyService.coreTopologyForDatabase( namedDatabaseId ), namedDatabaseId, topologyService );
        var rrTopology = serverInfo( topologyService.readReplicaTopologyForDatabase( namedDatabaseId ), namedDatabaseId, topologyService );
        return new ClusterServerInfos( coreTopology, rrTopology, leaderService.getLeaderId( namedDatabaseId ).orElse( null ) );
    }

    private static ClusterServerInfos.ServerInfos serverInfo( Topology<? extends DiscoveryServerInfo> topology, NamedDatabaseId databaseId,
            TopologyService topologyService )
    {
        return new ClusterServerInfos.ServerInfos( topology.servers()
                .entrySet()
                .stream()
                .map( ClusterServerInfosProvider::newServerInfo )
                .collect(
                        Collectors.toMap( Function.identity(),
                                serverInfo -> topologyService.lookupDatabaseState( databaseId, serverInfo.serverId() ).operatorState() ) ) );
    }

    private static ServerInfo newServerInfo( Map.Entry<ServerId,? extends DiscoveryServerInfo> entry )
    {
        return newServerInfo( entry.getKey(), entry.getValue() );
    }

    private static ServerInfo newServerInfo( ServerId serverId, DiscoveryServerInfo discoveryServerInfo )
    {
        return new ServerInfo( discoveryServerInfo.connectors().clientBoltAddress(), serverId, discoveryServerInfo.groups() );
    }
}
