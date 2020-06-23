/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;

public class GetStoreIdRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( new NetworkReadableChannel( byteBuf ) );
        out.add( new GetStoreIdRequest( databaseId ) );
    }
}
