/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.handshake.ServerHandshakeFinishedEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;

@ChannelHandler.Sharable
public class InstalledProtocolHandler extends ChannelInboundHandlerAdapter
{
    private ConcurrentMap<SocketAddress,ProtocolStack> installedProtocols = new ConcurrentHashMap<>();

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( evt instanceof ServerHandshakeFinishedEvent.Created )
        {
            ServerHandshakeFinishedEvent.Created created = (ServerHandshakeFinishedEvent.Created) evt;
            installedProtocols.put( created.advertisedSocketAddress, created.protocolStack );
        }
        else if ( evt instanceof ServerHandshakeFinishedEvent.Closed )
        {
            ServerHandshakeFinishedEvent.Closed closed = (ServerHandshakeFinishedEvent.Closed) evt;
            installedProtocols.remove( closed.advertisedSocketAddress );
        }
        else
        {
            super.userEventTriggered( ctx, evt );
        }
    }

    public Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        return installedProtocols.entrySet().stream().map( entry -> Pair.of( entry.getKey(), entry.getValue() ) );
    }
}
