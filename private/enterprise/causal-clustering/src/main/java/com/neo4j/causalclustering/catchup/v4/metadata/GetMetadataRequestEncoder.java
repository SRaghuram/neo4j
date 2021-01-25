/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetMetadataRequestEncoder extends MessageToByteEncoder<GetMetadataRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetMetadataRequest msg, ByteBuf out ) throws Exception
    {
        StringMarshal.marshal( out, msg.databaseName );
        StringMarshal.marshal( out, msg.includeMetadata.name() );
    }
}
