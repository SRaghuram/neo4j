/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.function.BiConsumer;

/**
 * Encodes messages sent by the server.
 */
public class ServerMessageEncoder extends MessageToByteEncoder<ClientMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out )
    {
        msg.dispatch( new Encoder( out ) );
    }

    class Encoder implements ClientMessageHandler
    {
        private final ByteBuf out;

        Encoder( ByteBuf out )
        {
            this.out = out;
        }

        @Override
        public void handle( ApplicationProtocolResponse applicationProtocolResponse )
        {
            out.writeInt( ApplicationProtocolResponse.MESSAGE_CODE );
            encodeProtocolResponse( applicationProtocolResponse, ( buf, version ) -> version.encode( buf ) );
        }

        @Override
        public void handle( ModifierProtocolResponse modifierProtocolResponse )
        {
            out.writeInt( ModifierProtocolResponse.MESSAGE_CODE );
            encodeProtocolResponse( modifierProtocolResponse, StringMarshal::marshal );
        }

        @Override
        public void handle( SwitchOverResponse switchOverResponse )
        {
            out.writeInt( SwitchOverResponse.MESSAGE_CODE );
            out.writeInt( switchOverResponse.status().codeValue() );
        }

        private <U extends Comparable<U>> void encodeProtocolResponse( BaseProtocolResponse<U> protocolResponse, BiConsumer<ByteBuf,U> writer )
        {
            out.writeInt( protocolResponse.statusCode().codeValue() );
            StringMarshal.marshal( out, protocolResponse.protocolName() );
            writer.accept( out, protocolResponse.version() );
        }
    }
}
