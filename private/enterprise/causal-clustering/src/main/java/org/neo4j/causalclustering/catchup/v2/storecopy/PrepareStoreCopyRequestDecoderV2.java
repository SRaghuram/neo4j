/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class PrepareStoreCopyRequestDecoderV2 extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list ) throws Exception
    {
        String databaseName = StringMarshal.unmarshal( byteBuf );
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( new NetworkReadableClosableChannelNetty4( byteBuf ) );
        list.add( new PrepareStoreCopyRequest( storeId, databaseName ) );
    }
}
