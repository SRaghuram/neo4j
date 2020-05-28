/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.configuration.ApplicationProtocolVersion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.function.TriFunction;

/**
 * Decodes messages received by the client.
 */
public class ClientMessageDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws ClientHandshakeException
    {
        int messageCode = in.readInt();

        switch ( messageCode )
        {
        case ApplicationProtocolResponse.MESSAGE_CODE:
        {
            ApplicationProtocolResponse applicationProtocolResponse = decodeProtocolResponse( ApplicationProtocolResponse::new,
                    ApplicationProtocolVersion::decode, in );

            out.add( applicationProtocolResponse );
            return;
        }
        case ModifierProtocolResponse.MESSAGE_CODE:
        {
            ModifierProtocolResponse modifierProtocolResponse = decodeProtocolResponse( ModifierProtocolResponse::new, StringMarshal::unmarshal, in );

            out.add( modifierProtocolResponse );
            return;
        }
        case SwitchOverResponse.MESSAGE_CODE:
        {
            int statusCodeValue = in.readInt();
            Optional<StatusCode> statusCode = StatusCode.fromCodeValue( statusCodeValue );
            if ( statusCode.isPresent() )
            {
                out.add( new SwitchOverResponse( statusCode.get() ) );
            }
            else
            {
                // TODO
            }
            return;
        }
        default:
            // TODO
            return;
        }
    }

    private <U extends Comparable<U>,T extends BaseProtocolResponse<U>> T decodeProtocolResponse( TriFunction<StatusCode,String,U,T> constructor,
            Function<ByteBuf,U> reader, ByteBuf in )
            throws ClientHandshakeException
    {
        int statusCodeValue = in.readInt();
        String identifier = StringMarshal.unmarshal( in );
        U version = reader.apply( in );

        Optional<StatusCode> statusCode = StatusCode.fromCodeValue( statusCodeValue );

        return statusCode
                .map( status -> constructor.apply( status, identifier, version ) )
                .orElseThrow( () -> new ClientHandshakeException(
                        String.format( "Unknown status code %s for protocol %s version %s", statusCodeValue, identifier, version ) ) );
    }
}
