/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class GetStoreIdRequestEncoder extends MessageToByteEncoder<GetStoreIdRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetStoreIdRequest msg, ByteBuf out )
    {
        out.writeByte( 0 );
    }
}
