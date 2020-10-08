/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InfoRequestHandlerTest
{
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private NamedDatabaseId databaseId;
    private InfoRequest databaseIdRequest;

    @BeforeEach
    void setUp()
    {
        databaseId = idRepository.getRaw( "foo" );
        databaseIdRequest = new InfoRequest( databaseId );
    }

    @Test
    void shouldResetExpectedProtocolState() throws Exception
    {
        var catchupServerProtocol = new CatchupServerProtocol();
        // currently expecting something else
        catchupServerProtocol.expect( CatchupServerProtocol.State.GET_CORE_SNAPSHOT );

        var databaseManager = mock( DatabaseManager.class );
        when( databaseManager.getDatabaseContext( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID ) ).thenReturn( Optional.empty() );

        var databaseStateService = mock( DatabaseStateService.class );
        when( databaseStateService.causeOfFailure( any() ) ).thenReturn( Optional.empty() );
        new InfoRequestHandler( catchupServerProtocol, databaseManager, databaseStateService ).channelRead0( mock( ChannelHandlerContext.class ),
                databaseIdRequest );

        assertThat( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) ).isTrue();
    }

    @Test
    void shouldReturnFailedIfContextIsNotPresent() throws Exception
    {
        var catchupServerProtocol = new CatchupServerProtocol();

        var databaseManager = mock( DatabaseManager.class );
        when( databaseManager.getDatabaseContext( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID ) ).thenReturn( Optional.empty() );
        when( databaseManager.databaseIdRepository() ).thenReturn( idRepository );
        var ctx = mock( ChannelHandlerContext.class );

        var databaseStateService = mock( DatabaseStateService.class );
        when( databaseStateService.causeOfFailure( any() ) ).thenReturn( Optional.empty() );
        new InfoRequestHandler( catchupServerProtocol, databaseManager, databaseStateService ).channelRead0( ctx, databaseIdRequest );

        verify( ctx ).write( ResponseMessageType.ERROR );
        verify( ctx ).writeAndFlush(
                new CatchupErrorResponse( CatchupResult.E_STORE_UNAVAILABLE,
                        "Unable to resolve local database for id " + DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID ) );
    }

    @Test
    void shouldReturnExpectedReconciledId() throws Exception
    {
        var catchupServerProtocol = new CatchupServerProtocol();
        // currently expecting something else
        catchupServerProtocol.expect( CatchupServerProtocol.State.GET_CORE_SNAPSHOT );

        long expected = 10L;

        DatabaseManager<?> databaseManager = mockDatabaseManager( expected );
        var ctx = mock( ChannelHandlerContext.class );

        var databaseStateService = mock( DatabaseStateService.class );
        when( databaseStateService.causeOfFailure( any() ) ).thenReturn( Optional.empty() );
        new InfoRequestHandler( catchupServerProtocol, databaseManager, databaseStateService ).channelRead0( ctx, databaseIdRequest );

        verify( ctx ).write( ResponseMessageType.INFO_RESPONSE );
        verify( ctx ).writeAndFlush( InfoResponse.create( expected, null ) );
    }

    @Test
    void shouldReturnExpectedReconciledIdWithStateFailure() throws Exception
    {
        var catchupServerProtocol = new CatchupServerProtocol();
        // currently expecting something else
        catchupServerProtocol.expect( CatchupServerProtocol.State.GET_CORE_SNAPSHOT );

        long expected = 10L;

        DatabaseManager<?> databaseManager = mockDatabaseManager( expected );
        var ctx = mock( ChannelHandlerContext.class );

        var databaseStateService = mock( DatabaseStateService.class );
        var reconciliationError = new RuntimeException( "This is an error" );
        when( databaseStateService.causeOfFailure( any() ) ).thenReturn( Optional.of( reconciliationError ) );
        new InfoRequestHandler( catchupServerProtocol, databaseManager, databaseStateService ).channelRead0( ctx, databaseIdRequest );

        verify( ctx ).write( ResponseMessageType.INFO_RESPONSE );
        verify( ctx ).writeAndFlush( InfoResponse.create( expected, reconciliationError.toString() ) );
    }

    private DatabaseManager<?> mockDatabaseManager( long expected )
    {
        ReconciledTransactionTracker tracker = mock( ReconciledTransactionTracker.class );
        when( tracker.getLastReconciledTransactionId() ).thenReturn( expected );
        var dependencies = mock( Dependencies.class );
        when( dependencies.resolveDependency( ReconciledTransactionTracker.class ) ).thenReturn( tracker );
        var dbContext = mock( DatabaseContext.class );
        when( dbContext.dependencies() ).thenReturn( dependencies );
        var databaseManager = mock( DatabaseManager.class );
        when( databaseManager.getDatabaseContext( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID ) ).thenReturn( Optional.of( dbContext ) );

        when( databaseManager.databaseIdRepository() ).thenReturn( idRepository );
        return databaseManager;
    }
}
