/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchUpResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupClientProtocol;

import static org.neo4j.causalclustering.catchup.CatchupClientProtocol.State;

public class StoreCopyFinishedResponseHandler extends SimpleChannelInboundHandler<StoreCopyFinishedResponse>
{
    private final CatchupClientProtocol protocol;
    private CatchUpResponseHandler handler;

    public StoreCopyFinishedResponseHandler( CatchupClientProtocol protocol,
                                             CatchUpResponseHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final StoreCopyFinishedResponse msg )
    {
        handler.onFileStreamingComplete( msg );
        protocol.expect( State.MESSAGE_TYPE );
    }
}
