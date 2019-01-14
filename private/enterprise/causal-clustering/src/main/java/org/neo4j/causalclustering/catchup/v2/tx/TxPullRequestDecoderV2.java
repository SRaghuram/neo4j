/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class TxPullRequestDecoderV2 extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out ) throws Exception
    {
        String databaseName = StringMarshal.unmarshal( byteBuf );
        long txId = byteBuf.readLong();
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( new NetworkReadableClosableChannelNetty4( byteBuf ) );
        out.add( new TxPullRequest( txId, storeId, databaseName ) );
    }
}
