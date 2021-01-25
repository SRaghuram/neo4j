/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class CoreSnapshotEncoder extends MessageToByteEncoder<CoreSnapshot>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CoreSnapshot coreSnapshot, ByteBuf out ) throws Exception
    {
        new CoreSnapshot.Marshal().marshal( coreSnapshot, new NetworkWritableChannel( out ) );
    }
}
