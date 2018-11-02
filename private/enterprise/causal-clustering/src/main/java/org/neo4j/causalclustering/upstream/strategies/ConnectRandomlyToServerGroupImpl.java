/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;

public class ConnectRandomlyToServerGroupImpl
{
    private final List<String> groups;
    private final TopologyService topologyService;
    private final MemberId myself;
    private final Random random = new Random();

    ConnectRandomlyToServerGroupImpl( List<String> groups, TopologyService topologyService, MemberId myself )
    {
        this.groups = groups;
        this.topologyService = topologyService;
        this.myself = myself;
    }

    public Optional<MemberId> upstreamDatabase()
    {
        Map<MemberId,ReadReplicaInfo> replicas = topologyService.localReadReplicas().members();

        List<MemberId> choices =
                groups.stream().flatMap( group -> replicas.entrySet().stream().filter( isMyGroupAndNotMe( group ) ) ).map( Map.Entry::getKey ).collect(
                        Collectors.toList() );

        if ( choices.isEmpty() )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( choices.get( random.nextInt( choices.size() ) ) );
        }
    }

    private Predicate<Map.Entry<MemberId,ReadReplicaInfo>> isMyGroupAndNotMe( String group )
    {
        return entry -> entry.getValue().groups().contains( group ) && !entry.getKey().equals( myself );
    }
}
