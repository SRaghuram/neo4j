/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.Optional.ofNullable;

public class DefaultRaftMembershipResolver implements RaftMembershipResolver
{
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private final TopologyService topologyService;

    public DefaultRaftMembershipResolver( TopologyService topologyService, DatabaseManager<ClusteredDatabaseContext> databaseManager )
    {
        this.topologyService = topologyService;
        this.databaseManager = databaseManager;
    }

    @Override
    public Set<ServerId> votingServers( NamedDatabaseId databaseId )
    {
        return databaseManager
                .getDatabaseContext( databaseId )
                .map( ctx -> ctx.dependencies().resolveDependency( RaftMembership.class ) )
                .map( RaftMembership::votingMembers )
                .orElse( Set.of() )
                .stream()
                .map( m -> ofNullable( topologyService.resolveServerForRaftMember( m ) ) )
                .flatMap( Optional::stream )
                .collect( Collectors.toSet() );
    }

    @Override
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId databaseId, ServerId to )
    {
        return topologyService.resolveRaftMemberForServer( databaseId, to );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId memberId )
    {
        return topologyService.resolveServerForRaftMember( memberId );
    }
}
