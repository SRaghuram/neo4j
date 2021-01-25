/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;

public class CoreSnapshotRequestEncoder extends MessageToByteEncoder<CoreSnapshotRequest>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, CoreSnapshotRequest msg, ByteBuf out ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( msg.databaseId(), new NetworkWritableChannel( out ) );
    }
}
