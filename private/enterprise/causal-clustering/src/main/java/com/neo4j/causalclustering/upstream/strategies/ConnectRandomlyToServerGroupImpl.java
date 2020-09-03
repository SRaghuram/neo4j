/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
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

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.stream.Collectors.toList;

public class ConnectRandomlyToServerGroupImpl
{
    private final Supplier<Set<ServerGroupName>> groupsProvider;
    private final TopologyService topologyService;
    private final ServerId myself;
    private final Random random = new Random();

    ConnectRandomlyToServerGroupImpl( Supplier<Set<ServerGroupName>> groupsProvider, TopologyService topologyService, ServerId myself )
    {
        this.groupsProvider = groupsProvider;
        this.topologyService = topologyService;
        this.myself = myself;
    }

    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
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

    public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var choices = choices( namedDatabaseId );
        Collections.shuffle( choices );
        return choices;
    }

    private List<ServerId> choices( NamedDatabaseId namedDatabaseId )
    {
        Map<ServerId,ReadReplicaInfo> replicas = topologyService.readReplicaTopologyForDatabase( namedDatabaseId ).servers();

        return groupsProvider.get()
                .stream()
                .flatMap( group -> replicas.entrySet().stream().filter( isMyGroupAndNotMe( group ) ) )
                .map( Map.Entry::getKey )
                .collect( toList() );
    }

    private Predicate<Map.Entry<ServerId,ReadReplicaInfo>> isMyGroupAndNotMe( ServerGroupName group )
    {
        return entry -> entry.getValue().groups().contains( group ) && !entry.getKey().equals( myself );
    }
}
