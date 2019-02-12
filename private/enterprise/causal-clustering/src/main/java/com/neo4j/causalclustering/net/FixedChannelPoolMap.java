/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;

class FixedChannelPoolMap extends AbstractChannelPoolMap<AdvertisedSocketAddress,ChannelPool>
{
    private final Bootstrap baseBootstrap;
    private final ChannelPoolHandlers poolHandlers = new ChannelPoolHandlers();
    private final InstalledProtocolsTracker protocolsTracker;

    FixedChannelPoolMap( Bootstrap baseBootstrap, ChannelPoolHandler poolHandlers )
    {
        this.baseBootstrap = baseBootstrap;
        this.protocolsTracker = new InstalledProtocolsTracker();
        this.poolHandlers.add( poolHandlers );
        this.poolHandlers.add( protocolsTracker );
    }

    Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        return protocolsTracker.installedProtocols();
    }

    @Override
    protected ChannelPool newPool( AdvertisedSocketAddress key )
    {
        return new FixedChannelPool( baseBootstrap.remoteAddress( key.socketAddress() ), poolHandlers, Integer.MAX_VALUE );
    }

    private static class InstalledProtocolsTracker extends AbstractChannelPoolHandler
    {
        private final Collection<Channel> createdChannels = new HashSet<>();

        Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
        {
            return createdChannels.stream().filter( Channel::isOpen ).map( ch ->
            {
                InetSocketAddress address = (InetSocketAddress) ch.remoteAddress();
                return Pair.of( new SocketAddress( address.getHostName(), address.getPort() ),
                        ch.attr( ChannelAttribute.PROTOCOL_STACK ).get().getNow( null ) );
            } ).filter( pair -> pair.other() != null );
        }

        @Override
        public void channelCreated( Channel ch )
        {
            createdChannels.add( ch );
            ch.closeFuture().addListener( f -> createdChannels.remove( ch ) );
        }
    }

    private static final class ChannelPoolHandlers implements ChannelPoolHandler
    {
        private final Collection<ChannelPoolHandler> poolHandlers = new ArrayList<>();

        @Override
        public void channelReleased( Channel ch )
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Channel released" ) )
            {
                poolHandlers.forEach( chh -> errorHandler.execute( () -> chh.channelReleased( ch ) ) );
            }
        }

        @Override
        public void channelAcquired( Channel ch )
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Channel acquired" ) )
            {
                poolHandlers.forEach( chh -> errorHandler.execute( () -> chh.channelAcquired( ch ) ) );
            }
        }

        @Override
        public void channelCreated( Channel ch )
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Channel created" ) )
            {
                poolHandlers.forEach( chh -> errorHandler.execute( () -> chh.channelCreated( ch ) ) );
            }
        }

        public void add( ChannelPoolHandler poolHandler )
        {
            poolHandlers.add( poolHandler );
        }
    }
}
