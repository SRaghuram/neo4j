/*
 * Copyright (c) "Neo4j"
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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;

/**
 * Decodes messages received by the server.
 */
public class ServerMessageDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        int messageCode = in.readInt();

        switch ( messageCode )
        {
        case ApplicationProtocolRequest.MESSAGE_CODE:
        {
            ApplicationProtocolRequest applicationProtocolRequest =
                    decodeProtocolRequest( ApplicationProtocolRequest::new, in, ApplicationProtocolVersion::decode );
            out.add( applicationProtocolRequest );
            return;
        }
        case ModifierProtocolRequest.MESSAGE_CODE:
            ModifierProtocolRequest modifierProtocolRequest = decodeProtocolRequest( ModifierProtocolRequest::new, in, StringMarshal::unmarshal );
            out.add( modifierProtocolRequest );
            return;
        case SwitchOverRequest.MESSAGE_CODE:
        {
            String protocolName = StringMarshal.unmarshal( in );
            ApplicationProtocolVersion version = ApplicationProtocolVersion.decode( in );
            int numberOfModifierProtocols = in.readInt();
            List<Pair<String,String>> modifierProtocols = Stream.generate( () -> Pair.of( StringMarshal.unmarshal( in ), StringMarshal.unmarshal( in ) ) )
                    .limit( numberOfModifierProtocols )
                    .collect( Collectors.toList() );
            out.add( new SwitchOverRequest( protocolName, version, modifierProtocols ) );
            return;
        }
        default:
            throw new IllegalStateException();
        }
    }

    private <U extends Comparable<U>, T extends BaseProtocolRequest<U>> T decodeProtocolRequest( BiFunction<String,Set<U>,T> constructor, ByteBuf in,
            Function<ByteBuf,U> versionDecoder )
    {
        String protocolName = StringMarshal.unmarshal( in );
        int versionArrayLength = in.readInt();

        Set<U> versions = Stream.generate( () -> versionDecoder.apply( in ) ).limit( versionArrayLength ).collect( Collectors.toSet() );

        return constructor.apply( protocolName, versions );
    }
}
