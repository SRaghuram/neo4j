/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiplexingCatchupRequestHandlerTest
{
    private static final DatabaseId EXISTING_DB_NAME = new DatabaseId( "existing.neo4j" );
    private static final DatabaseId NON_EXISTING_DB_NAME = new DatabaseId( "non.existing.neo4j" );
    private static final String SUCCESS_RESPONSE = "Correct handler invoked";

    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldReportErrorWhenDatabaseDoesNotExist()
    {
        channel.pipeline().addLast( newMultiplexingHandler() );
        CatchupProtocolMessage request = newCatchupRequest( NON_EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        assertThat( channel.readOutbound(), instanceOf( CatchupErrorResponse.class ) );
    }

    @Test
    void shouldReportErrorWhenPerDatabaseHandlerCantBeCreated()
    {
        Function<Database,SimpleChannelInboundHandler<CatchupProtocolMessage>> handlerFactory = ignore -> null;
        channel.pipeline().addLast( newMultiplexingHandler( handlerFactory ) );
        CatchupProtocolMessage request = newCatchupRequest( EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        assertThat( channel.readOutbound(), instanceOf( CatchupErrorResponse.class ) );
    }

    @Test
    void shouldInvokePerDatabaseHandler()
    {
        channel.pipeline().addLast( newMultiplexingHandler() );
        CatchupProtocolMessage request = newCatchupRequest( EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( SUCCESS_RESPONSE, channel.readOutbound() );
    }

    private static DatabaseManager<?> newDbManager()
    {
        StubClusteredDatabaseManager dbManager = new StubClusteredDatabaseManager();
        dbManager.givenDatabaseWithConfig()
                .withDatabaseId( EXISTING_DB_NAME )
                .register();
        return dbManager;
    }

    private static MultiplexingCatchupRequestHandler<CatchupProtocolMessage> newMultiplexingHandler()
    {
        return newMultiplexingHandler( MultiplexingCatchupRequestHandlerTest::newHandlerFactory );
    }

    private static MultiplexingCatchupRequestHandler<CatchupProtocolMessage> newMultiplexingHandler(
            Function<Database,SimpleChannelInboundHandler<CatchupProtocolMessage>> handlerFactory )
    {
        return new MultiplexingCatchupRequestHandler<>( new CatchupServerProtocol(), newDbManager(), handlerFactory,
                CatchupProtocolMessage.class, NullLogProvider.getInstance() );
    }

    private static SimpleChannelInboundHandler<CatchupProtocolMessage> newHandlerFactory( Database db )
    {
        assertEquals( EXISTING_DB_NAME, db.getDatabaseId() );
        return new CatchupProtocolMessageHandler();
    }

    private static CatchupProtocolMessage newCatchupRequest( DatabaseId databaseId )
    {
        return new DummyMessage( databaseId );
    }

    private static class CatchupProtocolMessageHandler extends SimpleChannelInboundHandler<CatchupProtocolMessage>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, CatchupProtocolMessage request )
        {
            ctx.writeAndFlush( SUCCESS_RESPONSE );
        }
    }

    private static class DummyMessage extends CatchupProtocolMessage
    {
        DummyMessage( DatabaseId databaseId )
        {
            super( RequestMessageType.STORE_FILE, databaseId );
        }
    }
}
