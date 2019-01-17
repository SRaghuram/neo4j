/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RequestDecoderDispatcher<E extends Enum<E>> extends ChannelInboundHandlerAdapter
{
    private final Map<E, ChannelInboundHandler> decoders = new HashMap<>();
    private final Protocol<E> protocol;
    private final Log log;

    public RequestDecoderDispatcher( Protocol<E> protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        ChannelInboundHandler delegate = protocol.select( decoders );
        if ( delegate == null )
        {
            log.warn( "Unregistered handler for protocol %s", protocol );

            /*
             * Since we cannot process this message further we need to release the message as per netty doc
             * see http://netty.io/wiki/reference-counted-objects.html#inbound-messages
             */
            ReferenceCountUtil.release( msg );
            return;
        }
        delegate.channelRead( ctx, msg );
    }

    public void register( E type, ChannelInboundHandler decoder )
    {
        assert !decoders.containsKey( type ) : "registering twice a decoder for the same type (" + type + ")?";
        decoders.put( type, decoder );
    }
}
