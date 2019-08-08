/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.kernel.database.DatabaseId;

public class GetDatabaseIdResponseEncoder extends MessageToByteEncoder<DatabaseId>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, DatabaseId msg, ByteBuf out ) throws Exception
    {
        DatabaseIdMarshal.INSTANCE.marshal( msg, new NetworkWritableChannel( out ) );
    }
}
