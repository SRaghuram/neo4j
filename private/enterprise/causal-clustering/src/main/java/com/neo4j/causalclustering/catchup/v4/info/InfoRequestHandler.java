/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.availability.UnavailableException;

public class InfoRequestHandler extends SimpleChannelInboundHandler<InfoRequest>
{
    private final CatchupServerProtocol protocol;
    private final InfoProvider infoProvider;

    public InfoRequestHandler( CatchupServerProtocol protocol, DatabaseManager<?> databaseManager,
            DatabaseStateService databaseStateService )
    {
        this.protocol = protocol;
        infoProvider = new InfoProvider( databaseManager, databaseStateService );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, InfoRequest msg ) throws Exception
    {
        try
        {
            var response = infoProvider.getInfo( msg.namedDatabaseId() );
            ctx.write( ResponseMessageType.INFO_RESPONSE );
            ctx.writeAndFlush( response );
        }
        catch ( UnavailableException exception )
        {
            sendError( ctx, CatchupResult.E_STORE_UNAVAILABLE, exception.getMessage() );
        }
        catch ( Throwable t )
        {
            sendError( ctx, CatchupResult.E_GENERAL_ERROR, t.toString() );
        }
        protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    private void sendError( ChannelHandlerContext ctx, CatchupResult status, String message )
    {
        ctx.write( ResponseMessageType.ERROR );
        ctx.writeAndFlush( new CatchupErrorResponse( status, message ) );
    }
}
