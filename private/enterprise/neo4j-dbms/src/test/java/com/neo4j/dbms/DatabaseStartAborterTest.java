/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.DatabaseStartAborter.PreventReason;
import com.neo4j.dbms.error_handling.DatabasePanicEvent;
import com.neo4j.dbms.error_handling.DatabasePanicReason;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.time.FakeClock;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED_DUMPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.function.Predicate.not;
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
    void shouldAbortWhenPanicked()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STARTED ) );
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when
        aborter.onPanic( new DatabasePanicEvent( databaseId, DatabasePanicReason.TEST, new RuntimeException() ) );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void panickedDatabaseShouldIgnoreDbmsModelDesiredState()
    {
        // given
        var ttl = Duration.ofSeconds( 5 );
        var clock = new FakeClock();
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STOPPED ) )
                                                 .thenReturn( Optional.of( STARTED ) );
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, clock, ttl );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
        clock.forward( ttl.plusMillis( 1 ) ); // Time out ttl
        assertFalse( aborter.shouldAbort( databaseId ) );

        // when
        aborter.onPanic( new DatabasePanicEvent( databaseId, DatabasePanicReason.TEST, new RuntimeException() ) );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
        clock.forward( ttl.plusMillis( 1 ) ); // Time out ttl but it won't matter
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void shouldAbortWhenPanickedEvenIfPrevented()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );

        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STOPPED ) );

        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when
        aborter.preventUserAborts( databaseId, PreventReason.STORE_COPY );

        // then
        assertFalse( aborter.shouldAbort( databaseId ) );

        // when
        aborter.onPanic( new DatabasePanicEvent( databaseId, DatabasePanicReason.TEST, new RuntimeException() ) );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void shouldClearAllPanicsOnReset()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STARTED ) );
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // when
        aborter.onPanic( new DatabasePanicEvent( databaseId, DatabasePanicReason.TEST, new RuntimeException() ) );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );

        // when
        aborter.resetFor( databaseId );

        // then
        assertFalse( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void shouldClearAllPreventionsOnReset()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        NamedDatabaseId databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STOPPED ) );
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );

        // when
        aborter.preventUserAborts( databaseId, PreventReason.STORE_COPY );

        // then
        assertFalse( aborter.shouldAbort( databaseId ) );

        // when
        aborter.resetFor( databaseId );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

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
        aborter.preventUserAborts( databaseId, PreventReason.STORE_COPY );

        // then
        assertFalse( aborter.shouldAbort( databaseId ) );

        // when
        aborter.allowUserAborts( databaseId, PreventReason.STORE_COPY );

        // then
        assertTrue( aborter.shouldAbort( databaseId ) );
    }

    @Test
    void shouldAbortIfGlobalAvailabilityShutDown()
    {
        // given
        var globalGuard = mock( AvailabilityGuard.class );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        var databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, new FakeClock(), Duration.ofSeconds( 5 ) );

        when( globalGuard.isShutdown() ).thenReturn( false ).thenReturn( true );
        when( dbmsModel.getStatus( databaseId ) ).thenReturn( Optional.of( STARTED ) );

        // when/then
        assertFalse( aborter.shouldAbort( databaseId ), "Should not abort initially" );
        assertTrue( aborter.shouldAbort( databaseId ), "Any database should abort in the event of global shutdown" );
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
        var stopDropStates = Set.of( STOPPED, DROPPED, DROPPED_DUMPED );
        var nonStopDrop = Stream.of( EnterpriseOperatorState.values() )
                .filter( not( stopDropStates::contains ) )
                .map( Optional::of )
                .collect( Collectors.toList() );

        var stopLast = new LinkedList<>( nonStopDrop );
        stopLast.add( Optional.of( STOPPED ) );

        var dropLast = new LinkedList<>( nonStopDrop );
        dropLast.add( Optional.of( DROPPED ) );

        var dropDumpLast = new LinkedList<>( nonStopDrop );
        dropDumpLast.add( Optional.of( DROPPED_DUMPED ) );

        var id1 = TestDatabaseIdRepository.randomNamedDatabaseId();
        var id2 = TestDatabaseIdRepository.randomNamedDatabaseId();
        var id3 = TestDatabaseIdRepository.randomNamedDatabaseId();

        var globalGuard = mock( AvailabilityGuard.class );
        when( globalGuard.isShutdown() ).thenReturn( false );
        var dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
        doReturn( stopLast.removeFirst(), stopLast.toArray() ).when( dbmsModel ).getStatus( id1 );
        doReturn( dropLast.removeFirst(), dropLast.toArray() ).when( dbmsModel ).getStatus( id2 );
        doReturn( dropDumpLast.removeFirst(), dropDumpLast.toArray() ).when( dbmsModel ).getStatus( id3 );
        var clock = new FakeClock();
        var aborter = new DatabaseStartAborter( globalGuard, dbmsModel, clock, Duration.ofSeconds( 1 ) );

        // when
        for ( int i = 0; i < nonStopDrop.size(); i++ )
        {
            assertFalse( aborter.shouldAbort( id1 ), "Database should not abort initially" );
            assertFalse( aborter.shouldAbort( id2 ), "Database should not abort initially" );
            assertFalse( aborter.shouldAbort( id3 ), "Database should not abort initially" );
            clock.forward( Duration.ofSeconds( 1 ) );
        }

        assertTrue( aborter.shouldAbort( id1 ), "Database should eventually abort" );
        assertTrue( aborter.shouldAbort( id2 ), "Database should eventually abort" );
        assertTrue( aborter.shouldAbort( id3 ), "Database should eventually abort" );

        // Each database is tested for each status except one of Stop, Drop or Drop Dump, so we expect OperatorState.values().length - 2 invocations
        verify( dbmsModel, times( EnterpriseOperatorState.values().length - 2 ) ).getStatus( id1 );
        verify( dbmsModel, times( EnterpriseOperatorState.values().length - 2 ) ).getStatus( id2 );
    }
}
