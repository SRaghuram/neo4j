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
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;

import static java.util.Collections.unmodifiableCollection;

class SimpleChannelPoolMap extends AbstractChannelPoolMap<AdvertisedSocketAddress,SimpleChannelPool>
{
    private final Bootstrap baseBootstrap;
    private final ChannelPoolHandlers poolHandlers;
    private final InstalledProtocolsTracker protocolsTracker;

    SimpleChannelPoolMap( Bootstrap baseBootstrap, ChannelPoolHandler poolHandler )
    {
        this.baseBootstrap = baseBootstrap;
        this.protocolsTracker = new InstalledProtocolsTracker();
        this.poolHandlers = new ChannelPoolHandlers( Arrays.asList( poolHandler, protocolsTracker ) );
    }

    Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        return protocolsTracker.installedProtocols();
    }

    @Override
    protected SimpleChannelPool newPool( AdvertisedSocketAddress key )
    {
        return new SimpleChannelPool( baseBootstrap.remoteAddress( key.socketAddress() ), poolHandlers );
    }

    private static class InstalledProtocolsTracker extends AbstractChannelPoolHandler
    {
        private final Collection<Channel> createdChannels = ConcurrentHashMap.newKeySet();

        Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
        {
            return createdChannels.stream().filter( Channel::isActive ).map( ch ->
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
        private final Collection<ChannelPoolHandler> poolHandlers;

        ChannelPoolHandlers( Collection<ChannelPoolHandler> poolHandlers )
        {
            Objects.requireNonNull( poolHandlers );
            this.poolHandlers = unmodifiableCollection( poolHandlers );
        }

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
    }
}
