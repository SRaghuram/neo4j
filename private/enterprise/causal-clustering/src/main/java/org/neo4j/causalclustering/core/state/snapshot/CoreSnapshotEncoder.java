/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.messaging.NetworkWritableChannel;

public class CoreSnapshotEncoder extends MessageToByteEncoder<CoreSnapshot>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CoreSnapshot coreSnapshot, ByteBuf out ) throws Exception
    {
        new CoreSnapshot.Marshal().marshal( coreSnapshot, new NetworkWritableChannel( out ) );
    }
}
