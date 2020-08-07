/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class ReplicatedContentChunkDecoder extends ByteToMessageDecoder
{
    private final Codec<ReplicatedContent> codec;
    private boolean isLast;

    public ReplicatedContentChunkDecoder()
    {
        setCumulator( new ContentChunkCumulator() );
        this.codec = new ReplicatedContentCodec();
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        if ( isLast )
        {
            out.add( codec.decode( in ) );
            isLast = false;
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
