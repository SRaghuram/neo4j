/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Address provider for catchup client.
 */
public interface CatchupAddressProvider
{
    /**
     * @return The address to the primary location where up to date requests are required. For a cluster aware provider the obvious choice would be the
     * leader address.
     * @throws CatchupAddressResolutionException if the provider was unable to find an address to this location.
     */
    SocketAddress primary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException;

    /**
     * @return The address to a secondary location that are not required to be up to date. If there are multiple secondary locations it is recommended to
     * do some simple load balancing for returned addresses. This is to avoid re-sending failed requests to the same instance immediately.
     * @throws CatchupAddressResolutionException if the provider was unable to find an address to this location.
     */
    SocketAddress secondary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException;

    class SingleAddressProvider implements CatchupAddressProvider
    {
        private final SocketAddress socketAddress;

        public SingleAddressProvider( SocketAddress socketAddress )
        {
            this.socketAddress = socketAddress;
        }

        @Override
        public SocketAddress primary( NamedDatabaseId namedDatabaseId )
        {
            return socketAddress;
        }

        @Override
        public SocketAddress secondary( NamedDatabaseId namedDatabaseId )
        {
            return socketAddress;
        }
    }

    class UpstreamStrategyBasedAddressProvider implements CatchupAddressProvider
    {
        private final UpstreamAddressLookup upstreamAddressLookup;

        public UpstreamStrategyBasedAddressProvider( TopologyService topologyService, UpstreamDatabaseStrategySelector strategySelector )
        {
            upstreamAddressLookup = new UpstreamAddressLookup( strategySelector, topologyService );
        }

        @Override
        public SocketAddress primary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        }

        @Override
        public SocketAddress secondary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        }
    }

    /**
     * Uses leader address as primary and given upstream strategy as secondary address.
     */
    class LeaderOrUpstreamStrategyBasedAddressProvider implements CatchupAddressProvider
    {
        private final LeaderLocator leaderLocator;
        private final TopologyService topologyService;
        private final UpstreamAddressLookup secondaryUpstreamAddressLookup;

        public LeaderOrUpstreamStrategyBasedAddressProvider( LeaderLocator leaderLocator, TopologyService topologyService,
                UpstreamDatabaseStrategySelector strategySelector )
        {
            this.leaderLocator = leaderLocator;
            this.topologyService = topologyService;
            this.secondaryUpstreamAddressLookup = new UpstreamAddressLookup( strategySelector, topologyService );
        }

        @Override
        public SocketAddress primary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            MemberId leadMember = leaderLocator.getLeader();
            if ( leadMember == null )
            {
                throw new CatchupAddressResolutionException( new IllegalStateException( "No Leader Found" ) );
            }
            return topologyService.lookupCatchupAddress( leadMember );
        }

        @Override
        public SocketAddress secondary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return secondaryUpstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        }
    }
}
