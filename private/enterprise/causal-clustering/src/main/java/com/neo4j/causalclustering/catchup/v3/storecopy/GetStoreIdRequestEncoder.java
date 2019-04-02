/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetStoreIdRequestEncoder extends MessageToByteEncoder<GetStoreIdRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetStoreIdRequest request, ByteBuf out ) throws Exception
    {
        DatabaseIdMarshal.INSTANCE.marshal( request.databaseId(), new NetworkWritableChannel( out ) );
    }
}
