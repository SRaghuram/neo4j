/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class CatchupErrorResponseEncoder extends MessageToByteEncoder<CatchupErrorResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CatchupErrorResponse msg, ByteBuf out )
    {
        out.writeInt( msg.status().ordinal() );
        StringMarshal.marshal( out, msg.message() );
    }
}
