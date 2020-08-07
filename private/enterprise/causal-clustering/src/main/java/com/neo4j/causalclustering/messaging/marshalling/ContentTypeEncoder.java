/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ContentTypeEncoder extends MessageToByteEncoder<ContentType>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ContentType msg, ByteBuf out )
    {
        out.writeByte( msg.get() );
    }
}
