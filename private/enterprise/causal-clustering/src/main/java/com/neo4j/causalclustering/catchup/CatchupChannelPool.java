/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

public class CatchupChannelPool extends LifecycleAdapter
{
    static final AttributeKey<TrackingResponseHandler> TRACKING_RESPONSE_HANDLER = AttributeKey.valueOf( "TRACKING_RESPONSE_HANDLER" );

    private final ChannelPoolService channelPoolService;

    CatchupChannelPool( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler jobScheduler, Clock clock,
            Function<CatchupResponseHandler,HandshakeClientInitializer> initializerFactory )
    {
        this.channelPoolService = new ChannelPoolService( bootstrapConfiguration, jobScheduler, new TrackingResponsePoolHandler( initializerFactory, clock ) );
    }

    CompletableFuture<CatchupChannel> acquire( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return channelPoolService.acquire( advertisedSocketAddress ).thenApply( CatchupChannel::new );
    }

    @Override
    public void start()
    {
        channelPoolService.start();
    }

    @Override
    public void stop()
    {
        channelPoolService.stop();
    }

    private static class TrackingResponsePoolHandler implements ChannelPoolHandler
    {
        private final Function<CatchupResponseHandler,HandshakeClientInitializer> initializerFactory;
        private final Clock clock;

        TrackingResponsePoolHandler( Function<CatchupResponseHandler,HandshakeClientInitializer> initializerFactory, Clock clock )
        {
            this.initializerFactory = initializerFactory;
            this.clock = clock;
        }

        @Override
        public void channelReleased( Channel ch )
        {
            ch.attr( TRACKING_RESPONSE_HANDLER ).get().clearResponseHandler();
        }

        @Override
        public void channelAcquired( Channel ch )
        {
            // do nothing
        }

        @Override
        public void channelCreated( Channel ch )
        {
            TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( clock );
            ch.pipeline().addLast( initializerFactory.apply( trackingResponseHandler ) );
            ch.attr( TRACKING_RESPONSE_HANDLER ).set( trackingResponseHandler );
            ch.closeFuture().addListener( f -> trackingResponseHandler.onClose() );
        }
    }
}
