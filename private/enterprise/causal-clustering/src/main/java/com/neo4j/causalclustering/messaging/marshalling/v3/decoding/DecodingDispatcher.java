/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v3.decoding;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.v2.ContentType;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.RaftLogEntryTermsDecoder;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.ReplicatedContentChunkDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.logging.LogProvider;

public class DecodingDispatcher extends RequestDecoderDispatcher<ContentType>
{
    public DecodingDispatcher( Protocol<ContentType> protocol, LogProvider logProvider )
    {
        super( protocol, logProvider );
        register( ContentType.ContentType, new ByteToMessageDecoder()
        {
            @Override
            protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
            {
                if ( in.readableBytes() > 0 )
                {
                    throw new IllegalStateException( "Not expecting any data here" );
                }
            }
        } );
        register( ContentType.RaftLogEntryTerms, new RaftLogEntryTermsDecoder( protocol ) );
        register( ContentType.ReplicatedContent, new ReplicatedContentChunkDecoder() );
        register( ContentType.Message, new RaftMessageDecoder( protocol ) );
    }
}
