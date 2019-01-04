/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.string.UTF8;

public class CatchupErrorResponseEncoder extends MessageToByteEncoder<CatchupErrorResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CatchupErrorResponse msg, ByteBuf out )
    {
        out.writeInt( msg.status().ordinal() );

        String message = msg.message();
        out.writeInt( message.length() );
        byte[] encodedBytes = UTF8.encode( message );
        out.writeBytes( encodedBytes );
    }
}
