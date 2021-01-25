/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.kernel.database.DatabaseId;

public class GetDatabaseIdResponseEncoder extends MessageToByteEncoder<DatabaseId>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, DatabaseId msg, ByteBuf out ) throws Exception
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( msg, new NetworkWritableChannel( out ) );
    }
}
