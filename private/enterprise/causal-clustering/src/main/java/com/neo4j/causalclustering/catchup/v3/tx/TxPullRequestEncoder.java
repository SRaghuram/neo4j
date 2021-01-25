/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class TxPullRequestEncoder extends MessageToByteEncoder<TxPullRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, TxPullRequest request, ByteBuf out ) throws Exception
    {
        NetworkWritableChannel channel = new NetworkWritableChannel( out );
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( request.databaseId(), channel );
        channel.putLong( request.previousTxId() );
        StoreIdMarshal.INSTANCE.marshal( request.expectedStoreId(), channel );
    }
}
