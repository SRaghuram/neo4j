/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetMetadataResponseEncoder extends MessageToByteEncoder<GetMetadataResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetMetadataResponse msg, ByteBuf out ) throws Exception
    {
        final var channel = new NetworkWritableChannel( out );
        channel.putLong( msg.commands.size() );

        msg.commands.forEach( command -> StringMarshal.marshal( out, command ) );
    }
}
