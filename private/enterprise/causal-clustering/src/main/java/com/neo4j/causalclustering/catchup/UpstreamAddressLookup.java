/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class UpstreamAddressLookup
{
    private final UpstreamDatabaseStrategySelector strategySelector;
    private final TopologyService topologyService;

    UpstreamAddressLookup( UpstreamDatabaseStrategySelector strategySelector, TopologyService topologyService )
    {
        this.strategySelector = strategySelector;
        this.topologyService = topologyService;
    }

    public SocketAddress lookupAddressForDatabase( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
    {
        try
        {
            ServerId upstreamServer = strategySelector.bestUpstreamServerForDatabase( namedDatabaseId );
            return topologyService.lookupCatchupAddress( upstreamServer );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            throw new CatchupAddressResolutionException( e );
        }
    }

    public Collection<SocketAddress> lookupAddressesForDatabase( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
    {
        try
        {
            var upstreamServers = strategySelector.bestUpstreamServersForDatabase( namedDatabaseId );
            var upstreamAddresses = new ArrayList<SocketAddress>();
            for ( var server : upstreamServers )
            {
                upstreamAddresses.add( topologyService.lookupCatchupAddress( server ) );
            }
            return upstreamAddresses;
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            throw new CatchupAddressResolutionException( e );
        }
    }
}
