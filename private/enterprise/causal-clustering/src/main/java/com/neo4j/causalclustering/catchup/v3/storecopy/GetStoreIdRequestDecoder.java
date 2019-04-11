/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.database.DatabaseId;

public class GetStoreIdRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( new NetworkReadableClosableChannelNetty4( byteBuf ) );
        out.add( new GetStoreIdRequest( databaseId ) );
    }
}
