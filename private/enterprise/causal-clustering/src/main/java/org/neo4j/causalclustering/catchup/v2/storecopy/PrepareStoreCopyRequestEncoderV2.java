/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

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
