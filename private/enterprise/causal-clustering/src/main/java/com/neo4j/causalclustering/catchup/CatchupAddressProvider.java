/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.core.LeaderProvider;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
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

    /**
     * @return the collection of addresses for known secondary locations which are not required to be up to date. Whilst it is possible to return a
     * stable list of all known addresses, many implementations will return changing sub lists according to load balancing policies or filtering rules.
     * @throws CatchupAddressResolutionException if the provider was unable to find any addresses to this location
     */
    default Collection<SocketAddress> allSecondaries( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
    {
        return List.of( secondary( namedDatabaseId ) );
    }

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

        @Override
        public Collection<SocketAddress> allSecondaries( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return upstreamAddressLookup.lookupAddressesForDatabase( namedDatabaseId );
        }
    }

    /**
     * Uses leader address as primary and given upstream strategy as secondary address.
     */
    class LeaderOrUpstreamStrategyBasedAddressProvider implements CatchupAddressProvider
    {
        private final LeaderProvider leaderProvider;
        private final TopologyService topologyService;
        private final UpstreamAddressLookup secondaryUpstreamAddressLookup;

        public LeaderOrUpstreamStrategyBasedAddressProvider( LeaderProvider leaderProvider, TopologyService topologyService,
                UpstreamDatabaseStrategySelector strategySelector )
        {
            this.leaderProvider = leaderProvider;
            this.topologyService = topologyService;
            this.secondaryUpstreamAddressLookup = new UpstreamAddressLookup( strategySelector, topologyService );
        }

        @Override
        public SocketAddress primary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            var leader = leaderProvider.getLeader( namedDatabaseId );
            if ( leader == null )
            {
                throw new CatchupAddressResolutionException( new IllegalStateException( "No Leader Found" ) );
            }
            ServerId server = topologyService.resolveServerForRaftMember( leader );
            return topologyService.lookupCatchupAddress( server );
        }

        @Override
        public SocketAddress secondary( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return secondaryUpstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        }

        @Override
        public Collection<SocketAddress> allSecondaries( NamedDatabaseId namedDatabaseId ) throws CatchupAddressResolutionException
        {
            return secondaryUpstreamAddressLookup.lookupAddressesForDatabase( namedDatabaseId );
        }
    }
}
