/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.configuration.ServerGroupName;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.configuration.CausalClusteringSettings.connect_randomly_to_server_group_strategy;

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
        config.addListener( connect_randomly_to_server_group_strategy, ( before, after ) -> logConfigUpdate( after ) );
        Supplier<Set<ServerGroupName>> groupsProvider = () -> Set.copyOf( config.get( connect_randomly_to_server_group_strategy ) );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groupsProvider, topologyService, myself );
        logConfigUpdate( groupsProvider.get() );
    }

    private void logConfigUpdate( Collection<ServerGroupName> currentGroups )
    {
        if ( currentGroups.isEmpty() )
        {
            log.warn( "No server groups configured for upstream strategy " + name +
                      ". Strategy will not find upstream servers until you have configured. %s",
                    connect_randomly_to_server_group_strategy.name() );
        }
        else
        {
            String readableGroups = String.join( ", ", currentGroups );
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
