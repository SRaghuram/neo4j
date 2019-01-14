/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchUpResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupClientProtocol;

public class FileChunkHandler extends SimpleChannelInboundHandler<FileChunk>
{
    private final CatchupClientProtocol protocol;
    private CatchUpResponseHandler handler;

    public FileChunkHandler( CatchupClientProtocol protocol, CatchUpResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, FileChunk fileChunk ) throws Exception
    {
        if ( handler.onFileContent( fileChunk ) )
        {
            protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
        }
    }
}
