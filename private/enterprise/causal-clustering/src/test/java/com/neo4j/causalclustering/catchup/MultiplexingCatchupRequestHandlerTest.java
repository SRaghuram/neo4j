/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.DatabaseCatchupRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MultiplexingCatchupRequestHandlerTest
{
    private static final String EXISTING_DB_NAME = "existing.graph.db";
    private static final String NON_EXISTING_DB_NAME = "non.existing.graph.db";
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
        DatabaseCatchupRequest request = newCatchupRequest( NON_EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        assertThat( channel.readOutbound(), instanceOf( CatchupErrorResponse.class ) );
    }

    @Test
    void shouldReportErrorWhenPerDatabaseHandlerCantBeCreated()
    {
        Function<Database,SimpleChannelInboundHandler<DatabaseCatchupRequest>> handlerFactory = ignore -> null;
        channel.pipeline().addLast( newMultiplexingHandler( handlerFactory ) );
        DatabaseCatchupRequest request = newCatchupRequest( EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        assertThat( channel.readOutbound(), instanceOf( CatchupErrorResponse.class ) );
    }

    @Test
    void shouldInvokePerDatabaseHandler()
    {
        channel.pipeline().addLast( newMultiplexingHandler() );
        DatabaseCatchupRequest request = newCatchupRequest( EXISTING_DB_NAME );

        channel.writeInbound( request );

        assertEquals( SUCCESS_RESPONSE, channel.readOutbound() );
    }

    private DatabaseManager newDbManager()
    {
        DatabaseManager dbManager = mock( DatabaseManager.class );
        DatabaseContext dbContext = mock( DatabaseContext.class );
        Database db = mock( Database.class );
        when( db.getDatabaseName() ).thenReturn( EXISTING_DB_NAME );
        when( dbContext.getDatabase() ).thenReturn( db );
        when( dbManager.getDatabaseContext( EXISTING_DB_NAME ) ).thenReturn( Optional.of( dbContext ) );
        return dbManager;
    }

    private MultiplexingCatchupRequestHandler<DatabaseCatchupRequest> newMultiplexingHandler()
    {
        return newMultiplexingHandler( this::newHandlerFactory );
    }

    private MultiplexingCatchupRequestHandler<DatabaseCatchupRequest> newMultiplexingHandler(
            Function<Database,SimpleChannelInboundHandler<DatabaseCatchupRequest>> handlerFactory )
    {
        return new MultiplexingCatchupRequestHandler<>( new CatchupServerProtocol(), this::newDbManager, handlerFactory,
                DatabaseCatchupRequest.class, NullLogProvider.getInstance() );
    }

    private SimpleChannelInboundHandler<DatabaseCatchupRequest> newHandlerFactory( Database db )
    {
        assertEquals( EXISTING_DB_NAME, db.getDatabaseName() );
        return new DatabaseCatchupRequestHandler();
    }

    private static DatabaseCatchupRequest newCatchupRequest( String dbName )
    {
        DatabaseCatchupRequest request = mock( DatabaseCatchupRequest.class );
        when( request.databaseName() ).thenReturn( dbName );
        return request;
    }

    private static class DatabaseCatchupRequestHandler extends SimpleChannelInboundHandler<DatabaseCatchupRequest>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, DatabaseCatchupRequest request )
        {
            ctx.writeAndFlush( SUCCESS_RESPONSE );
        }
    }
}
