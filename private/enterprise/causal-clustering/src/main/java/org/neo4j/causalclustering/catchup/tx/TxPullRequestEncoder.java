/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class TxPullRequestEncoder extends MessageToByteEncoder<TxPullRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, TxPullRequest request, ByteBuf out ) throws Exception
    {
        out.writeLong( request.previousTxId() );
        StoreIdMarshal.INSTANCE.marshal( request.expectedStoreId(), new BoundedNetworkWritableChannel( out ) );
    }
}
