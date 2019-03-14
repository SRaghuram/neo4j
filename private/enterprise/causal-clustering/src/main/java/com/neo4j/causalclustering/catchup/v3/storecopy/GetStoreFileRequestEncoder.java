/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetStoreFileRequestEncoder extends MessageToByteEncoder<GetStoreFileRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetStoreFileRequest msg, ByteBuf out ) throws Exception
    {
        StringMarshal.marshal( out, msg.databaseName() );
        StoreIdMarshal.INSTANCE.marshal( msg.expectedStoreId(), new NetworkWritableChannel( out ) );
        out.writeLong( msg.requiredTransactionId() );
        String name = msg.file().getName();
        StringMarshal.marshal( out, name );
    }
}
