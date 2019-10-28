/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.time.FakeClock;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

class DatabaseStartAborterTest
{

    @Test
    void shouldAbortIfGlobalAvailabilityShutDown()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( true );
        var aborter = new DatabaseStartAborter( globalGuard, mock( EnterpriseSystemGraphDbmsModel.class ), new FakeClock(), Duration.ofSeconds( 5 ) );

        // when/then
        assertTrue( aborter.shouldAbort( TestDatabaseIdRepository.randomDatabaseId() ), "Any database should abort in the event of global shutdown" );
    }

    @Test
    void shouldNotQuerySystemDbForAbortingSystemDb()
    {
        // given
        var globaGuard = mock( AvailabilityGuard.class );
        when( globaGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        when( dbmsModel.getStatus( any( DatabaseId.class ) ) ).thenReturn( Optional.of( STARTED ) );
        var aborter = new DatabaseStartAborter( globaGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when/then
        var otherId = TestDatabaseIdRepository.randomDatabaseId();
        aborter.shouldAbort( otherId );
        verify( dbmsModel ).getStatus( otherId );
        verifyZeroInteractions( dbmsModel );
        aborter.shouldAbort( SYSTEM_DATABASE_ID );
    }

    @Test
    void shouldNotQuerySystemDbWhenStateCached()
    {
        // given
        var globaGuard = mock( AvailabilityGuard.class );
        when( globaGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        var clock = new FakeClock();
        when( dbmsModel.getStatus( any( DatabaseId.class ) ) ).thenReturn( Optional.of( STARTED ) ).thenReturn( Optional.of( STOPPED ) );
        var ttlSeconds = 5;
        var aborter = new DatabaseStartAborter( globaGuard, dbmsModel, clock, Duration.ofSeconds( ttlSeconds ) );

        // when/then
        var otherId = TestDatabaseIdRepository.randomDatabaseId();

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
        var nonStopDrop = Stream.of( OperatorState.values() )
                .filter( state -> state != STOPPED && state != DROPPED )
                .map( Optional::of )
                .collect( Collectors.toList() );

        var stopLast = new LinkedList<>( nonStopDrop );
        stopLast.add( Optional.of( STOPPED ) );

        var dropLast = new LinkedList<>( nonStopDrop );
        dropLast.add( Optional.of( DROPPED ) );

        var id1 = TestDatabaseIdRepository.randomDatabaseId();
        var id2 = TestDatabaseIdRepository.randomDatabaseId();

        var globaGuard = mock( AvailabilityGuard.class );
        when( globaGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        doReturn( stopLast.removeFirst(), stopLast.toArray() ).when( dbmsModel ).getStatus( id1 );
        doReturn( dropLast.removeFirst(), dropLast.toArray() ).when( dbmsModel ).getStatus( id2 );
        var clock = new FakeClock();
        var aborter = new DatabaseStartAborter( globaGuard, dbmsModel, clock, Duration.ofSeconds( 1 ) );

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
        verify( dbmsModel, times( OperatorState.values().length - 1 ) ).getStatus( id1 );
        verify( dbmsModel, times( OperatorState.values().length - 1 ) ).getStatus( id2 );
    }
}
