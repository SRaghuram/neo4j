/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.net.PooledChannel;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import io.netty.channel.Channel;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

class CatchupChannel
{
    private final PooledChannel pooledChannel;
    private TrackingResponseHandler trackingResponseHandler;

    CatchupChannel( PooledChannel pooledChannel )
    {
        this.pooledChannel = pooledChannel;
    }

    void setResponseHandler( CatchupResponseCallback<?> handler, CompletableFuture<?> requestOutcomeSignal )
    {
        getOrCreateResponseHandler().setResponseHandler( handler, requestOutcomeSignal );
    }

    ApplicationProtocol protocol()
    {
        return pooledChannel.protocolStack().applicationProtocol();
    }

    void send( CatchupProtocolMessage message )
    {
        Channel channel = pooledChannel.channel();
        channel.eventLoop().execute( () ->
        {
            channel.write( message.messageType(), channel.voidPromise() );
            channel.writeAndFlush( message, channel.voidPromise() );
        } );
    }

    void release()
    {
        pooledChannel.release();
    }

    OptionalLong millisSinceLastResponse()
    {
        return getOrCreateResponseHandler().millisSinceLastResponse();
    }

    private TrackingResponseHandler getOrCreateResponseHandler()
    {
        if ( trackingResponseHandler == null )
        {
            trackingResponseHandler = pooledChannel.getAttribute( CatchupChannelPoolService.TRACKING_RESPONSE_HANDLER );
        }
        return trackingResponseHandler;
    }

    void dispose()
    {
        pooledChannel.dispose();
    }
}
