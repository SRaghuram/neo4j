/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class TxPullRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out ) throws Exception
    {
        NetworkReadableChannel channel = new NetworkReadableChannel( byteBuf );
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        long txId = byteBuf.readLong();
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        out.add( new TxPullRequest( txId, storeId, databaseId ) );
    }
}
