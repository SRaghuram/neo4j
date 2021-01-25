/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetDatabaseIdRequestEncoder extends MessageToByteEncoder<GetDatabaseIdRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetDatabaseIdRequest msg, ByteBuf out ) throws Exception
    {
        StringMarshal.marshal( out, msg.databaseName() );
    }
}
