/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.error;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

class UnavailableDatabaseHandlerTest
{
    private final DatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId().databaseId();
    private final CatchupProtocolMessage.WithDatabaseId message = new GetStoreIdRequest( databaseId );
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final CatchupServerProtocol protocol = new CatchupServerProtocol();
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @BeforeEach
    void setUp()
    {
        protocol.expect( CatchupServerProtocol.State.GET_STORE_ID );
        var handler = new UnavailableDatabaseHandler<>( message.getClass(), protocol, availabilityGuard, logProvider );
        channel.pipeline().addLast( handler );
    }

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldLogWarningWhenUnavailable()
    {
        testLogWarning( "database", databaseId.toString(), "unavailable" );
    }

    @Test
    void shouldLogWarningWhenShutdown()
    {
        when( availabilityGuard.isShutdown() ).thenReturn( true );
        testLogWarning( "database", databaseId.toString(), "shutdown" );
    }

    @Test
    void shouldWriteErrorResponseWhenUnavailable()
    {
        testErrorResponse( "database", databaseId.toString(), "unavailable" );
    }

    @Test
    void shouldWriteErrorResponseWhenShutdown()
    {
        when( availabilityGuard.isShutdown() ).thenReturn( true );
        testErrorResponse( "database", databaseId.toString(), "shutdown" );
    }

    @Test
    void shouldUpdateProtocolExpectation()
    {
        channel.writeInbound( message );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    private void testLogWarning( String... messageComponents )
    {
        channel.writeInbound( message );

        assertThat( logProvider ).forClass(  UnavailableDatabaseHandler.class  ).containsMessages( messageComponents );
    }

    private void testErrorResponse( String... messageComponents )
    {
        channel.writeInbound( message );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        CatchupErrorResponse response = channel.readOutbound();
        assertEquals( CatchupResult.E_STORE_UNAVAILABLE, response.status() );
        assertThat( response.message(), matchesAllOf( messageComponents ) );
    }

    static Matcher<String> matchesAllOf( String... messageComponents )
    {
        return allOf( Stream.of( messageComponents ).map( Matchers::containsString ).collect( Collectors.toList() ) );
    }
}
