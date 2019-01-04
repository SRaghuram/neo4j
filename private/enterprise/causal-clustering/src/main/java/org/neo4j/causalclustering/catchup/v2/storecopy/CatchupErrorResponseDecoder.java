/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.CatchupErrorResponse;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.string.UTF8;

public class CatchupErrorResponseDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        int statusOrdinal = in.readInt();
        CatchupResult result = CatchupResult.values()[statusOrdinal];
        byte[] messageBytes = new byte[in.readInt()];
        in.readBytes( messageBytes );
        String errorMessage = UTF8.decode( messageBytes );
        out.add( new CatchupErrorResponse( result, errorMessage ) );
    }
}
