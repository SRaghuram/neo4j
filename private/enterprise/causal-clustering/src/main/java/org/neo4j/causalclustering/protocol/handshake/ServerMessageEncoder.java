/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.function.BiConsumer;

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

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
        public void handle( InitialMagicMessage magicMessage )
        {
            out.writeInt( InitialMagicMessage.MESSAGE_CODE );
            StringMarshal.marshal( out, magicMessage.magic() );
        }

        @Override
        public void handle( ApplicationProtocolResponse applicationProtocolResponse )
        {
            out.writeInt( 0 );
            encodeProtocolResponse( applicationProtocolResponse, ByteBuf::writeInt );
        }

        @Override
        public void handle( ModifierProtocolResponse modifierProtocolResponse )
        {
            out.writeInt( 1 );
            encodeProtocolResponse( modifierProtocolResponse, StringMarshal::marshal );
        }

        @Override
        public void handle( SwitchOverResponse switchOverResponse )
        {
            out.writeInt( 2 );
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
