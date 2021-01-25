/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.databaseid;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class GetDatabaseIdRequestHandlerTest
{
    private final DatabaseIdRepository.Caching databaseIdRepository = mock( DatabaseIdRepository.Caching.class );
    private final DatabaseManager<DatabaseContext> databaseManager = mock( DatabaseManager.class );
    private final CatchupServerProtocol protocol = new CatchupServerProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void beforeEach()
    {
        when( databaseManager.databaseIdRepository() ).thenReturn( databaseIdRepository );

        var handler = new GetDatabaseIdRequestHandler( databaseManager, protocol );
        channel.pipeline().addLast( handler );

    }

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldWriteDatabaseIdForKnownDatabaseName()
    {
        var databaseName = "foo";
        var namedDatabaseId = randomNamedDatabaseId();
        var request = new GetDatabaseIdRequest( databaseName );
        var dbContext = mock( DatabaseContext.class );
        var database = mock( Database.class );
        when( database.isStarted() ).thenReturn( true );
        when( dbContext.database() ).thenReturn( database );
        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( dbContext ) );
        when( databaseIdRepository.getByName( databaseName ) ).thenReturn( Optional.of( namedDatabaseId ) );

        assertFalse( channel.writeInbound( request ) );

        assertEquals( ResponseMessageType.DATABASE_ID_RESPONSE, channel.readOutbound() );
        assertEquals( namedDatabaseId.databaseId(), channel.readOutbound() );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldWriteErrorForUnknownDatabaseName()
    {
        var databaseName = "bar";
        var request = new GetDatabaseIdRequest( databaseName );
        var dbContext = mock( DatabaseContext.class );
        var database = mock( Database.class );
        when( database.isStarted() ).thenReturn( true );
        when( dbContext.database() ).thenReturn( database );
        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( dbContext ) );
        when( databaseIdRepository.getByName( databaseName ) ).thenReturn( Optional.empty() );

        assertFalse( channel.writeInbound( request ) );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        CatchupErrorResponse errorResponse = channel.readOutbound();
        assertEquals( CatchupResult.E_DATABASE_UNKNOWN, errorResponse.status() );
        assertThat( errorResponse.message(), containsString( "Database '" + databaseName + "' does not exist" ) );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldWriteErrorForStoppedDB()
    {
        var databaseName = "bar";
        var namedDatabaseId = randomNamedDatabaseId();
        var request = new GetDatabaseIdRequest( databaseName );
        var dbContext = mock( DatabaseContext.class );
        var database = mock( Database.class );
        when( database.isStarted() ).thenReturn( false );
        when( dbContext.database() ).thenReturn( database );
        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( dbContext ) );
        when( databaseIdRepository.getByName( databaseName ) ).thenReturn( Optional.of( namedDatabaseId ) );

        assertFalse( channel.writeInbound( request ) );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        CatchupErrorResponse errorResponse = channel.readOutbound();
        assertEquals( CatchupResult.E_STORE_UNAVAILABLE, errorResponse.status() );
        assertThat( errorResponse.message(), containsString( "Database '" + databaseName + "' is stopped. Start the database before backup" ) );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );

    }
}
