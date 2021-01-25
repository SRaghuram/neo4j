/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.identity.RaftGroupId;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.neo4j.configuration.helpers.SocketAddress;

/**
 * A {@link TrackingChannelPoolMap} with a fixed number of {@link ChannelPool} per {@link RaftGroupSocket}.
 * <p/>
 * This class creates a fixed number of {@link ChannelPool} for a given {@link RaftGroupSocket}.
 * <p>
 * After that limit has been reached it round-robins across the previously created {@link ChannelPool} for each new {@link RaftGroupSocket}.
 * </p>
 */
public class LoadBalancedTrackingChannelPoolMap extends TrackingChannelPoolMap<LoadBalancedTrackingChannelPoolMap.RaftGroupSocket>
{

    private final int maxChannels;
    private final Map<SocketAddress,List<ChannelPool>> channels;
    private final Map<SocketAddress,Integer> currentChannel;
    private final BiFunction<SocketAddress,Integer,Integer> roundRobin;

    public LoadBalancedTrackingChannelPoolMap( Bootstrap baseBootstrap,
                                               ChannelPoolHandler poolHandler,
                                               ChannelPoolFactory poolFactory,
                                               int maxChannels )
    {
        super( baseBootstrap, poolHandler, poolFactory, RaftGroupSocket::unresolvedSocketAddress );
        this.channels = new HashMap<>();
        this.currentChannel = new HashMap<>();
        this.maxChannels = maxChannels;
        this.roundRobin = ( k, v ) -> v == null ? 0 : (v + 1) % maxChannels;
    }

    @Override
    protected synchronized ChannelPool newPool( RaftGroupSocket raftGroupSocket )
    {
        var socketAddress = raftGroupSocket.socketAddress;
        var socketChannels = this.channels.computeIfAbsent( socketAddress, ignored -> new ArrayList<>( maxChannels ) );
        if ( socketChannels.size() < maxChannels )
        {
            return createNewChannel( raftGroupSocket, socketChannels );
        }
        else
        {
            return getNextChannel( socketAddress, socketChannels );
        }
    }

    private ChannelPool createNewChannel( RaftGroupSocket raftGroupSocket, List<ChannelPool> socketChannels )
    {
        var channelPool = super.newPool( raftGroupSocket );
        socketChannels.add( channelPool );
        return channelPool;
    }

    private ChannelPool getNextChannel( SocketAddress socketAddress, List<ChannelPool> socketChannels )
    {
        var channelIdx = this.currentChannel.compute( socketAddress, roundRobin );
        return socketChannels.get( channelIdx );
    }

    public static class RaftGroupSocket
    {
        private final RaftGroupId groupId;
        private final SocketAddress socketAddress;

        public RaftGroupSocket( RaftGroupId groupId, SocketAddress socketAddress )
        {
            this.groupId = groupId;
            this.socketAddress = socketAddress;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RaftGroupSocket that = (RaftGroupSocket) o;
            return Objects.equals( groupId, that.groupId ) &&
                   Objects.equals( socketAddress, that.socketAddress );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( groupId, socketAddress );
        }

        public InetSocketAddress unresolvedSocketAddress()
        {
            return InetSocketAddress.createUnresolved( socketAddress().getHostname(), socketAddress().getPort() );
        }

        public SocketAddress socketAddress()
        {
            return socketAddress;
        }
    }
}
