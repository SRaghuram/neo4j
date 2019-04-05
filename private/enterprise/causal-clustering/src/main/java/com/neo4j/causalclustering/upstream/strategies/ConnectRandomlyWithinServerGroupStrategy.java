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
        List<String> groups = config.get( CausalClusteringSettings.server_groups );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groups, topologyService, myself );
        log.warn( "Upstream selection strategy " + name + " is deprecated. Consider using " + IDENTITY + " instead." );
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
    {
        return strategyImpl.upstreamMemberForDatabase( databaseId );
    }
}
