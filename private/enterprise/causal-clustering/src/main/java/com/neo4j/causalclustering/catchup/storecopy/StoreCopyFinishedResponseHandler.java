/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.neo4j.causalclustering.catchup.CatchupClientProtocol.State;

public class StoreCopyFinishedResponseHandler extends SimpleChannelInboundHandler<StoreCopyFinishedResponse>
{
    private final CatchupClientProtocol protocol;
    private CatchupResponseHandler handler;

    public StoreCopyFinishedResponseHandler( CatchupClientProtocol protocol,
                                             CatchupResponseHandler handler )
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
