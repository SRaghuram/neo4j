/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.identity.StoreId;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;

public class GetStoreIdRequestHandler extends SimpleChannelInboundHandler<GetStoreIdRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<StoreId> storeIdSupplier;

    public GetStoreIdRequestHandler( CatchupServerProtocol protocol, Supplier<StoreId> storeIdSupplier )
    {
        this.protocol = protocol;
        this.storeIdSupplier = storeIdSupplier;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreIdRequest msg )
    {
        ctx.writeAndFlush( ResponseMessageType.STORE_ID );
        ctx.writeAndFlush( storeIdSupplier.get() );
        protocol.expect( State.MESSAGE_TYPE );
    }
}
