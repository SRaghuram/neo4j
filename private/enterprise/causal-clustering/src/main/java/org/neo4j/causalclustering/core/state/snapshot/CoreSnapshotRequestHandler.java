/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;

public class CoreSnapshotRequestHandler extends SimpleChannelInboundHandler<CoreSnapshotRequest>
{
    private final CatchupServerProtocol protocol;
    private final CoreSnapshotService snapshotService;

    public CoreSnapshotRequestHandler( CatchupServerProtocol protocol, CoreSnapshotService snapshotService )
    {
        this.protocol = protocol;
        this.snapshotService = snapshotService;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, CoreSnapshotRequest msg ) throws Exception
    {
        sendStates( ctx, snapshotService.snapshot() );
        protocol.expect( State.MESSAGE_TYPE );
    }

    private void sendStates( ChannelHandlerContext ctx, CoreSnapshot coreSnapshot )
    {
        ctx.writeAndFlush( ResponseMessageType.CORE_SNAPSHOT );
        ctx.writeAndFlush( coreSnapshot );
    }
}
