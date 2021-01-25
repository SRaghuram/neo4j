/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.discovery.akka.marshal.InefficientNamedDatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class InfoRequestEncoder extends MessageToByteEncoder<InfoRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, InfoRequest msg, ByteBuf out ) throws Exception
    {
        InefficientNamedDatabaseIdMarshal.INSTANCE.marshal( msg.namedDatabaseId(), new NetworkWritableChannel( out ) );
    }
}
