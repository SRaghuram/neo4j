/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.configuration.ServerGroupName;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.stream.Collectors.toList;

public class ConnectRandomlyToServerGroupImpl
{
    private final Supplier<Set<ServerGroupName>> groupsProvider;
    private final TopologyService topologyService;
    private final MemberId myself;
    private final Random random = new Random();

    ConnectRandomlyToServerGroupImpl( Supplier<Set<ServerGroupName>> groupsProvider, TopologyService topologyService, MemberId myself )
    {
        this.groupsProvider = groupsProvider;
        this.topologyService = topologyService;
        this.myself = myself;
    }

    public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var choices = choices( namedDatabaseId );

        if ( choices.isEmpty() )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( choices.get( random.nextInt( choices.size() ) ) );
        }
    }

    public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var choices = choices( namedDatabaseId );
        Collections.shuffle( choices );
        return choices;
    }

    private List<MemberId> choices( NamedDatabaseId namedDatabaseId )
    {
        Map<MemberId,ReadReplicaInfo> replicas = topologyService.readReplicaTopologyForDatabase( namedDatabaseId ).members();

        return groupsProvider.get()
                .stream()
                .flatMap( group -> replicas.entrySet().stream().filter( isMyGroupAndNotMe( group ) ) )
                .map( Map.Entry::getKey )
                .collect( toList() );
    }

    private Predicate<Map.Entry<MemberId,ReadReplicaInfo>> isMyGroupAndNotMe( ServerGroupName group )
    {
        return entry -> entry.getValue().groups().contains( group ) && !entry.getKey().equals( myself );
    }
}
