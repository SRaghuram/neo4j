/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

class InitMagicMessageEncoder extends MessageToByteEncoder<InitialMagicMessage>
{
    static final String NAME = "init_magic_message_encoder";

    @Override
    protected void encode( ChannelHandlerContext ctx, InitialMagicMessage msg, ByteBuf out )
    {
        out.writeInt( InitialMagicMessage.MESSAGE_CODE );
        StringMarshal.marshal( out, msg.magic() );
    }
}
