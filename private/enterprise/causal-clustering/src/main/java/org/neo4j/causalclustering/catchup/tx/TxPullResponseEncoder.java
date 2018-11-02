/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

public class TxPullResponseEncoder extends MessageToByteEncoder<TxPullResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, TxPullResponse response, ByteBuf out ) throws Exception
    {
        NetworkWritableChannel channel = new NetworkWritableChannel( out );
        StoreIdMarshal.INSTANCE.marshal( response.storeId(), channel );
        new LogEntryWriter( channel ).serialize( response.tx() );
    }
}
