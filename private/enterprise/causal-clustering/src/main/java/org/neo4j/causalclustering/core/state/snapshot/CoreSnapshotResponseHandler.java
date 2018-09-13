/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupResponseHandler;
import org.neo4j.causalclustering.catchup.CatchupClientProtocol;

public class CoreSnapshotResponseHandler extends SimpleChannelInboundHandler<CoreSnapshot>
{
    private final CatchupClientProtocol protocol;
    private final CatchupResponseHandler listener;

    public CoreSnapshotResponseHandler( CatchupClientProtocol protocol, CatchupResponseHandler listener )
    {
        this.protocol = protocol;
        this.listener = listener;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final CoreSnapshot coreSnapshot )
    {
        if ( protocol.isExpecting( CatchupClientProtocol.State.CORE_SNAPSHOT ) )
        {
            listener.onCoreSnapshot( coreSnapshot );
            protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
        }
        else
        {
            ctx.fireChannelRead( coreSnapshot );
        }
    }
}
