/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.identity.ServerId;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

class ClusterServerInfos
{
    private final ServerInfos coreTopology;
    private final ServerInfos followers;
    private final ServerInfos rrTopology;
    private final ServerInfos leader;

    ClusterServerInfos( ServerInfos coreTopology, ServerInfos rrTopology, ServerId actualLeaderId )
    {
        this.rrTopology = rrTopology;
        this.coreTopology = coreTopology;
        this.followers = new ServerInfos( new HashMap<>( coreTopology.serverInfos ) );
        this.leader = actualLeaderId == null ? new ServerInfos( Map.of() ) :
                      new ServerInfos( coreTopology.serverInfos.entrySet()
                              .stream()
                              .filter( entry -> entry.getKey().serverId().equals( actualLeaderId ) )
                              .findFirst()
                              .stream()
                              .collect( toMap( Map.Entry::getKey, Map.Entry::getValue ) ) );
        leader.allServers().forEach( followers.serverInfos::remove );
    }

    ServerInfos readReplicas()
    {
        return rrTopology;
    }

    ServerInfos followers()
    {
        return followers;
    }

    ServerInfos leader()
    {
        return leader;
    }

    ServerInfos cores()
    {
        return coreTopology;
    }

    static class ServerInfos
    {
        private final Map<ServerInfo,? extends OperatorState> serverInfos;

        ServerInfos( Map<ServerInfo,? extends OperatorState> serverInfos )
        {
            this.serverInfos = serverInfos;
        }

        Set<ServerInfo> allServers()
        {
            return Set.copyOf( serverInfos.keySet() );
        }

        Set<ServerInfo> onlineServers()
        {
            return serverInfos.entrySet()
                    .stream()
                    .filter( entry -> entry.getValue() == STARTED )
                    .map( Map.Entry::getKey )
                    .collect( toUnmodifiableSet() );
        }
    }
}
