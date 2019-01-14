/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ResponseMessageTypeEncoder extends MessageToByteEncoder<ResponseMessageType>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ResponseMessageType response, ByteBuf out )
    {
        out.writeByte( response.messageType() );
    }
}
