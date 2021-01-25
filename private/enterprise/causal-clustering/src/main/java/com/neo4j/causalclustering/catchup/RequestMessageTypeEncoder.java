/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RequestMessageTypeEncoder extends MessageToByteEncoder<RequestMessageType>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, RequestMessageType request, ByteBuf out )
    {
        out.writeByte( request.messageType() );
    }
}
