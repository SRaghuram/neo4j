/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupClientProtocol;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.CatchupClientProtocol.State;

public class FileHeaderHandler extends SimpleChannelInboundHandler<FileHeader>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler handler;
    private final Log log;

    public FileHeaderHandler( CatchupClientProtocol protocol, CatchupResponseHandler handler, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, FileHeader fileHeader )
    {
        log.info( "Receiving file: %s", fileHeader.fileName() );
        handler.onFileHeader( fileHeader );
        protocol.expect( State.FILE_CONTENTS );
    }
}
