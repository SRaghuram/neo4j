/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.ConnectException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;

public class CatchupClientFactory extends LifecycleAdapter
{
    private final Log log;
    private final Clock clock;
    private final CatchupChannelPool<CatchupChannel> pool = new CatchupChannelPool<>( CatchupChannel::new );
    private final Function<CatchupResponseHandler,HandshakeClientInitializer> channelInitializerFactory;
    private final String defaultDatabaseName;
    private final JobScheduler scheduler;
    private final Duration inactivityTimeout;

    private NioEventLoopGroup eventLoopGroup;

    public CatchupClientFactory( LogProvider logProvider, Clock clock, Function<CatchupResponseHandler,HandshakeClientInitializer> channelInitializerFactory,
            String defaultDatabaseName, Duration inactivityTimeout, JobScheduler scheduler )
    {
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.channelInitializerFactory = channelInitializerFactory;
        this.defaultDatabaseName = defaultDatabaseName;
        this.scheduler = scheduler;
        this.inactivityTimeout = inactivityTimeout;
    }

    public VersionedCatchupClients getClient( AdvertisedSocketAddress upstream ) throws Exception
    {
        return new CatchupClient( upstream, pool, defaultDatabaseName, inactivityTimeout );
    }

    class CatchupChannel implements CatchupChannelPool.Channel
    {
        private final TrackingResponseHandler handler;
        private final AdvertisedSocketAddress destination;
        private Channel nettyChannel;
        private final Bootstrap bootstrap;
        private final HandshakeClientInitializer channelInitializer;
        private Future<ApplicationProtocol> protocol;

        CatchupChannel( AdvertisedSocketAddress destination )
        {
            this.destination = destination;
            this.handler = new TrackingResponseHandler( clock );
            this.channelInitializer = channelInitializerFactory.apply( handler );
            this.bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( channelInitializer );
        }

        void clearResponseHandler()
        {
            handler.clearResponseHandler();
        }

        void setResponseHandler( CatchupResponseCallback responseHandler, CompletableFuture<?> requestOutcomeSignal )
        {
            handler.setResponseHandler( responseHandler, requestOutcomeSignal );
        }

        void send( CatchupProtocolMessage request ) throws ConnectException
        {
            if ( !isActive() )
            {
                throw new ConnectException( "Channel is not connected" );
            }
            nettyChannel.write( request.messageType() );
            nettyChannel.writeAndFlush( request );
        }

        Optional<Long> millisSinceLastResponse()
        {
            return handler.lastResponseTime().map( responseMillis -> clock.millis() - responseMillis );
        }

        @Override
        public AdvertisedSocketAddress destination()
        {
            return destination;
        }

        @Override
        public void connect() throws Exception
        {
            ChannelFuture channelFuture = bootstrap.connect( destination.socketAddress() );
            nettyChannel = channelFuture.sync().channel();
            nettyChannel.closeFuture().addListener( (ChannelFutureListener) future -> handler.onClose() );
            protocol = nettyChannel.attr( PROTOCOL_STACK ).get().thenApply( ProtocolStack::applicationProtocol );
        }

        @Override
        public boolean isActive()
        {
            return nettyChannel.isActive();
        }

        @Override
        public void close()
        {
            if ( nettyChannel != null )
            {
                nettyChannel.close();
            }
        }

        Future<ApplicationProtocol> protocol()
        {
            return protocol;
        }
    }

    @Override
    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup( 0, scheduler.executor( Group.CATCHUP_CLIENT ) );
    }

    @Override
    public void stop()
    {
        log.info( "CatchUpClient stopping" );
        try
        {
            pool.close();
            eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
        }
        catch ( InterruptedException e )
        {
            log.warn( "Interrupted while stopping catch up client." );
        }
    }
}
