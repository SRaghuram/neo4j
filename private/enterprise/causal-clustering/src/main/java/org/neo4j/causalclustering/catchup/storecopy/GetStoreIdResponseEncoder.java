/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class GetStoreIdResponseEncoder extends MessageToByteEncoder<StoreId>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, StoreId storeId, ByteBuf out ) throws Exception
    {
        StoreIdMarshal.INSTANCE.marshal( storeId, new NetworkWritableChannel( out ) );
    }
}
