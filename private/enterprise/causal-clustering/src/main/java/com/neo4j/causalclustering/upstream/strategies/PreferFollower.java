/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Strategy will pick an upstream server based on the priority defined in the rolePriorityMap.
 * Roles with higher priority will have be picked first.
 * */
@ServiceProvider
public class PreferFollower extends UpstreamDatabaseSelectionStrategy
{
    private static final String IDENTITY = "prefer-follower";
    private final Map<RoleInfo,Integer> rolePriorityMap;

    public PreferFollower()
    {
        super( IDENTITY );
        this.rolePriorityMap = Map.of(
                RoleInfo.FOLLOWER, 20,
                RoleInfo.LEADER, 15,
                RoleInfo.READ_REPLICA, 10,
                RoleInfo.UNKNOWN, 5 );

        verifyThatAllRolesAreRegistered( rolePriorityMap );
    }

    private void verifyThatAllRolesAreRegistered( Map<RoleInfo,Integer> rolePriorityMap )
    {
        if ( rolePriorityMap.keySet().size() != RoleInfo.values().length )
        {
            throw new IllegalStateException( "Not all roles are registered in role priority map" );
        }
    }

    @Override
    public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        return topologyService.coreTopologyForDatabase( namedDatabaseId )
                                           .servers()
                                           .keySet()
                                           .stream()
                                           .map( serverId -> new ClusterMember( serverId, topologyService.lookupRole( namedDatabaseId, serverId ) ) )
                                           .sorted()
                                           .findFirst().map( member -> member.serverId );
    }

    private class ClusterMember implements Comparable<ClusterMember>
    {
        public final ServerId serverId;
        private final int priority;

        private ClusterMember( ServerId serverId, RoleInfo roleInfo )
        {
            this.serverId = serverId;
            this.priority = rolePriorityMap.get( roleInfo );
        }

        @Override
        public int compareTo( ClusterMember otherMember )
        {
            return -1 * Integer.compare( priority, otherMember.priority ); // compare in reverse order
        }
    }
}
