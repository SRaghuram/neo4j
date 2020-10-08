/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ServerRequestTest
{
    @Test
    void shouldLogAndIgnoreAnyException()
    {
        var log = mock( Log.class );
        var exception = new Exception();
        var serverId = ServerId.of( UUID.randomUUID() );
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        ServerRequest serverRequest = new ServerRequest( 1, serverId, new SocketAddress( 1 ), databaseId, log )
        {
            @Override
            protected InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
            {
                throw exception;
            }
        };

        var serverResponse = serverRequest.call();

        assertThat( serverResponse ).isNull();
        verify( log ).info( "Failed to get reconciliation info for " + databaseId + " from " + serverId, exception );
    }

    @Test
    void shouldProvideCaughtUpFailure()
    {
        var log = mock( Log.class );
        var serverId = ServerId.of( UUID.randomUUID() );
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var address = new SocketAddress( 1 );
        ServerRequest serverRequest = new ServerRequest( 1, serverId, address, databaseId, log )
        {
            @Override
            protected InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
            {
                return InfoResponse.create( 1, "foo" );
            }
        };

        var serverResponse = serverRequest.call();
        assertThat( serverResponse )
                .matches( response -> response.serverId().equals( serverId ) )
                .matches( response -> response.address().equals( address ) )
                .matches( response -> response.state().equals( WaitResponseState.Failed ) )
                .matches( response -> response.message().equals( "Caught up but has failure for " + databaseId + " Failure: foo" ) );
    }

    @Test
    void shouldProvideNotCaughtUpFailure()
    {
        var log = mock( Log.class );
        var serverId = ServerId.of( UUID.randomUUID() );
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var address = new SocketAddress( 1 );
        ServerRequest serverRequest = new ServerRequest( 1, serverId, address, databaseId, log )
        {
            @Override
            protected InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
            {
                return InfoResponse.create( 0, "foo" );
            }
        };

        var serverResponse = serverRequest.call();
        assertThat( serverResponse )
                .matches( response -> response.serverId().equals( serverId ) )
                .matches( response -> response.address().equals( address ) )
                .matches( response -> response.state().equals( WaitResponseState.Failed ) )
                .matches( response -> response.message().equals( "Not caught up and has failure for " + databaseId + " Failure: foo" ) );
    }

    @Test
    void shouldReturnNullOnNotCaughtUp()
    {
        var log = mock( Log.class );
        var serverId = ServerId.of( UUID.randomUUID() );
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var address = new SocketAddress( 1 );
        ServerRequest serverRequest = new ServerRequest( 1, serverId, address, databaseId, log )
        {
            @Override
            protected InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
            {
                return InfoResponse.create( 0, null );
            }
        };

        var serverResponse = serverRequest.call();
        assertThat( serverResponse ).isNull();
    }

    @Test
    void shouldProvideCaughtUp()
    {
        var log = mock( Log.class );
        var serverId = ServerId.of( UUID.randomUUID() );
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var address = new SocketAddress( 1 );
        ServerRequest serverRequest = new ServerRequest( 1, serverId, address, databaseId, log )
        {
            @Override
            protected InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
            {
                return InfoResponse.create( 1, null );
            }
        };

        var serverResponse = serverRequest.call();
        assertThat( serverResponse )
                .matches( response -> response.serverId().equals( serverId ) )
                .matches( response -> response.address().equals( address ) )
                .matches( response -> response.state().equals( WaitResponseState.CaughtUp ) )
                .matches( response -> response.message().equals( "caught up" ) );
    }
}
