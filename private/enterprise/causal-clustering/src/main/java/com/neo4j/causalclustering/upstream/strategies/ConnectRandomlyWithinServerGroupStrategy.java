/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.NamedDatabaseId;

@ServiceProvider
public class ConnectRandomlyWithinServerGroupStrategy extends UpstreamDatabaseSelectionStrategy
{
    private static final String IDENTITY = "connect-randomly-within-server-group";

    private ConnectRandomlyToServerGroupImpl strategyImpl;

    public ConnectRandomlyWithinServerGroupStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        Set<ServerGroupName> groups = Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groups, topologyService, myself );
        log.warn( "Upstream selection strategy " + name + " is deprecated. Consider using " + IDENTITY + " instead." );
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return strategyImpl.upstreamMemberForDatabase( namedDatabaseId );
    }

    @Override
    public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return strategyImpl.upstreamMembersForDatabase( namedDatabaseId );
    }
}
