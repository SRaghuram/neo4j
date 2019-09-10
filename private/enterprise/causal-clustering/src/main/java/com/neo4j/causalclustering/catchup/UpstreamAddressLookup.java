/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import org.neo4j.configuration.helpers.SocketAddress;
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
            MemberId upstreamMember = strategySelector.bestUpstreamMemberForDatabase( namedDatabaseId );
            return topologyService.lookupCatchupAddress( upstreamMember );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            throw new CatchupAddressResolutionException( e );
        }
    }
}
