/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v1.storecopy;

import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PrepareStoreCopyRequestEncoder extends MessageToByteEncoder<PrepareStoreCopyRequest>
{
    @Override
    protected void encode( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest, ByteBuf byteBuf ) throws Exception
    {
        StoreIdMarshal.INSTANCE.marshal( prepareStoreCopyRequest.getStoreId(), new BoundedNetworkWritableChannel( byteBuf ) );
    }
}
