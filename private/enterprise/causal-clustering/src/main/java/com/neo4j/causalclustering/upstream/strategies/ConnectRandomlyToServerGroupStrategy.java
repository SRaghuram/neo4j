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
public class ConnectRandomlyToServerGroupStrategy extends UpstreamDatabaseSelectionStrategy
{
    static final String IDENTITY = "connect-randomly-to-server-group";
    private ConnectRandomlyToServerGroupImpl strategyImpl;

    public ConnectRandomlyToServerGroupStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        Set<ServerGroupName> groups = Set.copyOf( config.get( CausalClusteringSettings.connect_randomly_to_server_group_strategy ) );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groups, topologyService, myself );

        if ( groups.isEmpty() )
        {
            log.warn( "No server groups configured for upstream strategy " + name + ". Strategy will not find upstream servers." );
        }
        else
        {
            String readableGroups = String.join( ", ", groups );
            log.info( "Upstream selection strategy " + name + " configured with server groups " + readableGroups );
        }
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
