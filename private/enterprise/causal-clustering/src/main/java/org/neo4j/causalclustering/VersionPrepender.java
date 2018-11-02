/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.Message;

public class VersionPrepender extends MessageToByteEncoder<ByteBuf>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out )
    {
        out.writeByte( Message.CURRENT_VERSION );
        out.writeBytes( msg, msg.readerIndex(), msg.readableBytes() );
    }
}
