/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v2.storecopy;

import com.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PrepareStoreCopyRequestEncoderV2 extends MessageToByteEncoder<PrepareStoreCopyRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, PrepareStoreCopyRequest request, ByteBuf byteBuf ) throws Exception
    {
        //TODO: Make every CatchupRequestV2 provide a Marshall and use that here
        StringMarshal.marshal( byteBuf, request.databaseName() );
        StoreIdMarshal.INSTANCE.marshal( request.getStoreId(), new NetworkWritableChannel( byteBuf ) );
    }
}
