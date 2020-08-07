/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.catchup.Protocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RaftLogEntryTermsDecoder extends ByteToMessageDecoder
{
    private final Protocol<ContentType> protocol;

    public RaftLogEntryTermsDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        int size = in.readInt();
        long[] terms = new long[size];
        for ( int i = 0; i < size; i++ )
        {
           terms[i] = in.readLong();
        }
        out.add( new RaftLogEntryTerms( terms ) );
        protocol.expect( ContentType.ContentType );
    }
}
