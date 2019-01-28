/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.net.PooledChannel;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.Channel;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

class CatchupChannel
{
    private final PooledChannel pooledChannel;
    private TrackingResponseHandler trackingResponseHandler;

    CatchupChannel( PooledChannel pooledChannel )
    {
        this.pooledChannel = pooledChannel;
    }

    void setResponseHandler( CatchupResponseCallback handler, CompletableFuture<?> completableFuture )
    {
        getOrCreateResponseHanlder().setResponseHandler( handler, completableFuture );
    }

    CompletableFuture<Protocol.ApplicationProtocol> protocol()
    {
        return pooledChannel.getAttribute( ChannelAttribute.PROTOCOL_STACK ).thenApply( ProtocolStack::applicationProtocol );
    }

    void send( CatchupProtocolMessage request )
    {
        Channel channel = pooledChannel.channel();
        channel.eventLoop().execute( () ->
        {
            channel.write( request.messageType() );
            channel.writeAndFlush( request );
        } );
    }

    Future<Void> release()
    {
        return pooledChannel.release();
    }

    Optional<Long> millisSinceLastResponse()
    {
        return getOrCreateResponseHanlder().millisSinceLastResponse();
    }

    private TrackingResponseHandler getOrCreateResponseHanlder()
    {
        if ( trackingResponseHandler == null )
        {
            trackingResponseHandler = pooledChannel.getAttribute( CatchupChannelPool.TRACKING_RESPONSE_HANDLER );
        }
        return trackingResponseHandler;
    }

    public void close()
    {
        pooledChannel.channel().close();
    }
}
