/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.kernel.database.DatabaseIdRepository;

public class GetDatabaseIdRequestHandler extends SimpleChannelInboundHandler<GetDatabaseIdRequest>
{
    private final CatchupServerProtocol protocol;
    private final DatabaseIdRepository databaseIdRepository;

    public GetDatabaseIdRequestHandler( DatabaseIdRepository databaseIdRepository, CatchupServerProtocol protocol )
    {
        this.protocol = protocol;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetDatabaseIdRequest msg ) throws Exception
    {
        ctx.writeAndFlush( ResponseMessageType.DATABASE_ID_RESPONSE );
        var databaseId = databaseIdRepository.get( msg.databaseName() );
        ctx.writeAndFlush( databaseId );
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }
}
