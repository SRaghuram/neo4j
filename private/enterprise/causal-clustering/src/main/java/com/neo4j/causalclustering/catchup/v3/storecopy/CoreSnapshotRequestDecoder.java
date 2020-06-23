/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.marshal.EndOfStreamException;

public class CoreSnapshotRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( new NetworkReadableChannel( in ) );
        out.add( new CoreSnapshotRequest( databaseId ) );
    }
}
