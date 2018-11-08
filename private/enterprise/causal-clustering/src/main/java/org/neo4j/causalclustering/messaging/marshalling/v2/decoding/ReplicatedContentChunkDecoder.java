/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.Codec;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;

public class ReplicatedContentChunkDecoder extends ByteToMessageDecoder
{
    private final Codec<ReplicatedContent> codec = CoreReplicatedContentMarshalFactory.codecV2();
    private boolean expectingNewContent = true;
    private boolean isLast;

    ReplicatedContentChunkDecoder()
    {
        setCumulator( new ContentChunkCumulator() );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        if ( expectingNewContent )
        {
            isLast = in.readBoolean();
            expectingNewContent = false;
        }
        if ( isLast )
        {
            out.add( codec.decode( in ) );
            isLast = false;
            expectingNewContent = true;
        }
    }

    private class ContentChunkCumulator implements Cumulator
    {
        @Override
        public ByteBuf cumulate( ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in )
        {
            isLast = in.readBoolean();
            return COMPOSITE_CUMULATOR.cumulate( alloc, cumulation, in );
        }
    }
}
