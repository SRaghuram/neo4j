/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class PrepareStoreCopyRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list ) throws Exception
    {
        NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( byteBuf );
        var databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        var storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        list.add( new PrepareStoreCopyRequest( storeId, databaseId ) );
    }
}
