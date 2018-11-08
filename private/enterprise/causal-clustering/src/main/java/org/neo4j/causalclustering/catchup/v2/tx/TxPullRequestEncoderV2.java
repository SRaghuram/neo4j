/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;

public class TxPullRequestEncoderV2 extends MessageToByteEncoder<TxPullRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, TxPullRequest request, ByteBuf out ) throws Exception
    {
        StringMarshal.marshal( out, request.databaseName() );
        out.writeLong( request.previousTxId() );
        StoreIdMarshal.INSTANCE.marshal( request.expectedStoreId(), new NetworkWritableChannel( out ) );
    }
}
