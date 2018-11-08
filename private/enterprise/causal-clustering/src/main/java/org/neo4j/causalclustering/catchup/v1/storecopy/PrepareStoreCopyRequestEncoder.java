/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class PrepareStoreCopyRequestEncoder extends MessageToByteEncoder<PrepareStoreCopyRequest>
{
    @Override
    protected void encode( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest, ByteBuf byteBuf ) throws Exception
    {
        StoreIdMarshal.INSTANCE.marshal( prepareStoreCopyRequest.getStoreId(), new BoundedNetworkWritableChannel( byteBuf ) );
    }
}
