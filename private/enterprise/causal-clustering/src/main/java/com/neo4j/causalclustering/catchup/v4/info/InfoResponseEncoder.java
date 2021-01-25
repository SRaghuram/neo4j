/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class InfoResponseEncoder extends MessageToByteEncoder<InfoResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, InfoResponse msg, ByteBuf out ) throws Exception
    {
        out.writeLong( msg.reconciledId() );
        out.writeBoolean( msg.reconciliationFailure().isPresent() );
        msg.reconciliationFailure().ifPresent( message -> StringMarshal.marshal( out, message ) );
    }
}
