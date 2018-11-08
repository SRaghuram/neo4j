/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

public class GetStoreIdRequestEncoderV2 extends MessageToByteEncoder<GetStoreIdRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetStoreIdRequest request, ByteBuf out ) throws Exception
    {
        StringMarshal.marshal( out, request.databaseName() );
    }
}
