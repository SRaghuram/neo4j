/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetAllDatabaseIdsRequestEncoder extends MessageToByteEncoder<GetAllDatabaseIdsRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetAllDatabaseIdsRequest msg, ByteBuf out ) throws Exception
    {
        out.writeByte( 1 );
    }
}
