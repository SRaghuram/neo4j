/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PrepareStoreCopyRequestEncoder extends MessageToByteEncoder<PrepareStoreCopyRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, PrepareStoreCopyRequest request, ByteBuf byteBuf ) throws Exception
    {
        NetworkWritableChannel channel = new NetworkWritableChannel( byteBuf );
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( request.databaseId(), channel );
        StoreIdMarshal.INSTANCE.marshal( request.storeId(), channel );
    }
}
