/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.catchup.CatchupClientProtocol.State;

public class FileHeaderHandler extends SimpleChannelInboundHandler<FileHeader>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;
    private final Log log;

    public FileHeaderHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler, Log log )
    {
        this.protocol = protocol;
        this.handler = handler;
        this.log = log;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, FileHeader fileHeader )
    {
        log.info( "Receiving file: %s from: %s", fileHeader.fileName(), ctx.channel().remoteAddress() );
        handler.onFileHeader( fileHeader );
        protocol.expect( State.FILE_CHUNK );
    }
}
