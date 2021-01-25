/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.DatabaseStartAborter.PreventReason;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.time.FakeClock;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class DatabaseStartAborterTest
{
    @Test
    void shouldNotAbortWhenPrevented()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );

        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STOPPED ) );

        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when
        aborter.setAbortable( databaseId, PreventReason.STORE_COPY, false );

        // then
        assertFalse( aborter.shouldAbort( databaseId ) );

        // when
        aborter.setAbortable( databaseId, PreventReason.STORE_COPY, true );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void shouldAbortIfGlobalAvailabilityShutDown()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( true );
        var aborter = new DatabaseStartAborter( globalGuard, mock( EnterpriseSystemGraphDbmsModel.class ), new FakeClock(), Duration.ofSeconds( 5 ) );

        // when/then
        assertTrue( aborter.shouldAbort( TestDatabaseIdRepository.randomNamedDatabaseId() ), "Any database should abort in the event of global shutdown" );
    }

    @Test
    void shouldNotQuerySystemDbForAbortingSystemDb()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        when( dbmsModel.getStatus( any( NamedDatabaseId.class ) ) ).thenReturn( Optional.of( STARTED ) );
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when/then
        var otherId = TestDatabaseIdRepository.randomNamedDatabaseId();
        aborter.shouldAbort( otherId );
        verify( dbmsModel ).getStatus( otherId );
        verifyNoMoreInteractions( dbmsModel );
        aborter.shouldAbort( NAMED_SYSTEM_DATABASE_ID );
    }

    @Test
    void shouldNotQuerySystemDbWhenStateCached()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        var clock = new FakeClock();
        when( dbmsModel.getStatus( any( NamedDatabaseId.class ) ) ).thenReturn( Optional.of( STARTED ) ).thenReturn( Optional.of( STOPPED ) );
        var ttlSeconds = 5;
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, clock, Duration.ofSeconds( ttlSeconds ) );

        // when/then
        var otherId = TestDatabaseIdRepository.randomNamedDatabaseId();

        for ( int i = 0; i < ttlSeconds; i++ )
        {
            assertFalse( aborter.shouldAbort( otherId ), "Database should not abort initially" );
            clock.forward( Duration.ofSeconds( 1 ) );
        }

        assertTrue( aborter.shouldAbort( otherId ), "Database should eventually abort" );
        verify( dbmsModel, times( 2 ) ).getStatus( otherId );
    }

    @Test
    void shouldOnlyAbortForStopDrop()
    {
        // given
        var nonStopDrop = Stream.of( EnterpriseOperatorState.values() )
                .filter( state -> state != STOPPED && state != DROPPED )
                .map( Optional::of )
                .collect( Collectors.toList() );

        var stopLast = new LinkedList<>( nonStopDrop );
        stopLast.add( Optional.of( STOPPED ) );

        var dropLast = new LinkedList<>( nonStopDrop );
        dropLast.add( Optional.of( DROPPED ) );

        var id1 = TestDatabaseIdRepository.randomNamedDatabaseId();
        var id2 = TestDatabaseIdRepository.randomNamedDatabaseId();

        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        doReturn( stopLast.removeFirst(), stopLast.toArray() ).when( dbmsModel ).getStatus( id1 );
        doReturn( dropLast.removeFirst(), dropLast.toArray() ).when( dbmsModel ).getStatus( id2 );
        var clock = new FakeClock();
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, clock, Duration.ofSeconds( 1 ) );

        // when
        for ( int i = 0; i < nonStopDrop.size(); i++ )
        {
            assertFalse( aborter.shouldAbort( id1 ), "Database should not abort initially" );
            assertFalse( aborter.shouldAbort( id2 ), "Database should not abort initially" );
            clock.forward( Duration.ofSeconds( 1 ) );
        }

        assertTrue( aborter.shouldAbort( id1 ), "Database should eventually abort" );
        assertTrue( aborter.shouldAbort( id2 ), "Database should eventually abort" );

        // Each database is tested for each status except one of Stop or Drop, so we expect OperatorState.values().length - 1 invocations
        verify( dbmsModel, times( EnterpriseOperatorState.values().length - 1 ) ).getStatus( id1 );
        verify( dbmsModel, times( EnterpriseOperatorState.values().length - 1 ) ).getStatus( id2 );
    }
}
