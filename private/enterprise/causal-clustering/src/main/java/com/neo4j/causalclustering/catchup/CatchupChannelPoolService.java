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
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;

import java.time.Clock;
import java.util.function.Function;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

class CatchupChannelPoolService extends ChannelPoolService
{
    static final AttributeKey<TrackingResponseHandler> TRACKING_RESPONSE_HANDLER = AttributeKey.valueOf( "TRACKING_RESPONSE_HANDLER" );

    CatchupChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler jobScheduler, Clock clock,
            Function<CatchupResponseHandler,HandshakeClientInitializer> initializerFactory )
    {
        super( bootstrapConfiguration, jobScheduler, Group.CATCHUP_CLIENT, new TrackingResponsePoolHandler( initializerFactory, clock ) );
    }

    private static class TrackingResponsePoolHandler extends AbstractChannelPoolHandler
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
        public void channelCreated( Channel ch )
        {
            TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( clock );
            ch.pipeline().addLast( initializerFactory.apply( trackingResponseHandler ) );
            ch.attr( TRACKING_RESPONSE_HANDLER ).set( trackingResponseHandler );
            ch.closeFuture().addListener( f -> trackingResponseHandler.onClose() );
        }
    }
}
