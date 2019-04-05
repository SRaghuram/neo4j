/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.List;
import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.DatabaseId;

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
        List<String> groups = config.get( CausalClusteringSettings.connect_randomly_to_server_group_strategy );
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
    public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
    {
        return strategyImpl.upstreamMemberForDatabase( databaseId );
    }
}
