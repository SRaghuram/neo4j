/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
 * Encodes messages sent by the client.
 */
public class ClientMessageEncoder extends MessageToByteEncoder<ServerMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ServerMessage msg, ByteBuf out )
    {
        msg.dispatch( new Encoder( out ) );
    }

    class Encoder implements ServerMessageHandler
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
        public void handle( ApplicationProtocolRequest applicationProtocolRequest )
        {
            out.writeInt( 1 );
            encodeProtocolRequest( applicationProtocolRequest, ByteBuf::writeInt );
        }

        @Override
        public void handle( ModifierProtocolRequest modifierProtocolRequest )
        {
            out.writeInt( 2 );
            encodeProtocolRequest( modifierProtocolRequest, StringMarshal::marshal );
        }

        @Override
        public void handle( SwitchOverRequest switchOverRequest )
        {
            out.writeInt( 3 );
            StringMarshal.marshal( out, switchOverRequest.protocolName() );
            out.writeInt( switchOverRequest.version() );
            out.writeInt( switchOverRequest.modifierProtocols().size() );
            switchOverRequest.modifierProtocols().forEach( pair ->
                    {
                        StringMarshal.marshal( out, pair.first() );
                        StringMarshal.marshal( out, pair.other() );
                    }
            );
        }

        private <U extends Comparable<U>> void encodeProtocolRequest( BaseProtocolRequest<U> protocolRequest, BiConsumer<ByteBuf,U> writer )
        {
            StringMarshal.marshal( out, protocolRequest.protocolName() );
            out.writeInt( protocolRequest.versions().size() );
            protocolRequest.versions().forEach( version -> writer.accept( out, version) );
        }
    }
}
