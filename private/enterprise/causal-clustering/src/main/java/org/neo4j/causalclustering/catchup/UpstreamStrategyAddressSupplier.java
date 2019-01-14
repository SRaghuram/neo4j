/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class UpstreamStrategyAddressSupplier implements ThrowingSupplier<AdvertisedSocketAddress,CatchupAddressResolutionException>
{
    private final UpstreamDatabaseStrategySelector strategySelector;
    private final TopologyService topologyService;

    UpstreamStrategyAddressSupplier( UpstreamDatabaseStrategySelector strategySelector, TopologyService topologyService )
    {
        this.strategySelector = strategySelector;
        this.topologyService = topologyService;
    }

    @Override
    public AdvertisedSocketAddress get() throws CatchupAddressResolutionException
    {
        try
        {
            MemberId upstreamMember = strategySelector.bestUpstreamDatabase();
            return topologyService.findCatchupAddress( upstreamMember );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            throw new CatchupAddressResolutionException( e );
        }
    }
}
