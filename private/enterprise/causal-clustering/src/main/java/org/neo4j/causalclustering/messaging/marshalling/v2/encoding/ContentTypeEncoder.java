/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2.encoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

public class ContentTypeEncoder extends MessageToByteEncoder<ContentType>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ContentType msg, ByteBuf out )
    {
        out.writeByte( msg.get() );
    }
}
