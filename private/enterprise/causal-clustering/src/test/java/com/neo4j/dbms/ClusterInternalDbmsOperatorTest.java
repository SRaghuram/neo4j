/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static com.neo4j.dbms.OperatorState.STORE_COPYING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class ClusterInternalDbmsOperatorTest
{
    private OperatorConnector connector = mock( OperatorConnector.class );
    private ClusterInternalDbmsOperator operator = new ClusterInternalDbmsOperator();

    @BeforeEach
    void setup()
    {
        when( connector.trigger( any( ReconcilerRequest.class ) ) ).thenReturn( ReconcilerResponse.EMPTY );
        operator.connect( connector );
    }

    @Test
    void shouldNotDesireAnythingInitially()
    {
        assertEquals( operator.desired(), Collections.emptyMap() );
    }

    @Test
    void shouldTriggerOnStartAndStop()
    {
        // given
        var someDb = randomDatabaseId();

        // when
        var stoppedDatabase = operator.stopForStoreCopy( someDb );

        // then
        verify( connector, times( 1 ) ).trigger( ReconcilerRequest.simple() );

        // when
        stoppedDatabase.restart();

        // then
        verify( connector, times( 2 ) ).trigger( ReconcilerRequest.simple() );
    }

    @Test
    void shouldReturnIndependentContexts()
    {
        // given
        var databaseA = randomDatabaseId();
        var databaseB = randomDatabaseId();

        // when
        var stopA1 = operator.stopForStoreCopy( databaseA );

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertNull( operator.desired().get( databaseB.name() ) );

        // when
        var stopA2 = operator.stopForStoreCopy( databaseA );

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertNull( operator.desired().get( databaseB.name() ) );

        // when
        var stopB1 = operator.stopForStoreCopy( databaseB );

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        var stopB2 = operator.stopForStoreCopy( databaseB );

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopA1.restart();

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopA2.restart();

        // then
        assertNull( operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopB1.restart();

        // then
        assertNull( operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopB2.restart();

        // then
        assertEquals( operator.desired(), Collections.emptyMap() );
    }

    @Test
    void shouldComplainWhenStartingTwice()
    {
        // given
        var someDb = randomDatabaseId();
        var stoppedDatabase = operator.stopForStoreCopy( someDb );
        stoppedDatabase.restart();

        // when/then
        assertThrows( IllegalStateException.class, stoppedDatabase::restart );
    }

    @Test
    void shouldNotDesireStoreCopyingWhileBootstrapping()
    {
        // given
        var someDb = randomDatabaseId();
        var bootstrapping = operator.bootstrap( someDb );

        // when
        var storeCopying = operator.stopForStoreCopy( someDb );

        // then
        assertNotEquals( STORE_COPYING, operatorState( someDb ) );

        // when
        bootstrapping.bootstrapped();

        // then
        assertEquals( STORE_COPYING, operatorState( someDb ) );

        // when
        storeCopying.restart();

        // then
        assertNull( operator.desired().get( someDb.name() ) );
    }

    private OperatorState operatorState( DatabaseId databaseId )
    {
        return Optional.ofNullable( operator.desired().get( databaseId.name() ) )
                .map( DatabaseState::operationalState )
                .orElse( null );
    }

    @Test
    void shouldDesireStopForPanickedDatabase()
    {
        // given
        var someDb = randomDatabaseId();
        var desiredStateOnTrigger = new AtomicReference<OperatorState>();
        captureDesiredStateWhenOperatorTriggered( someDb, desiredStateOnTrigger );

        // when
        operator.stopOnPanic( someDb );

        // then
        assertEquals( STOPPED, desiredStateOnTrigger.get() );
        verify( connector ).trigger( ReconcilerRequest.forPanickedDatabase( someDb ) );
    }

    @Test
    void shouldDesireStopForPanickedDatabaseWhileStoreCoping()
    {
        // given
        var someDb = randomDatabaseId();
        var desiredStateOnTrigger = new AtomicReference<OperatorState>();
        captureDesiredStateWhenOperatorTriggered( someDb, desiredStateOnTrigger );

        // when
        operator.stopForStoreCopy( someDb );
        operator.stopOnPanic( someDb );

        // then
        assertEquals( STOPPED, desiredStateOnTrigger.get() );
    }

    @Test
    void shouldNotTriggerRestartAfterStoreCopyWhenPanicked()
    {
        // given
        var someDb = randomDatabaseId();

        // when
        operator.stopOnPanic( someDb );
        var storeCopyHandle = operator.stopForStoreCopy( someDb );
        storeCopyHandle.restart();

        // then
        verify( connector ).trigger( ReconcilerRequest.forPanickedDatabase( someDb ) );
    }

    private void captureDesiredStateWhenOperatorTriggered( DatabaseId databaseId, AtomicReference<OperatorState> stateRef )
    {
        when( connector.trigger( any( ReconcilerRequest.class ) ) ).thenAnswer( invocation ->
        {
            var desiredState = operator.desired().get( databaseId.name() );
            stateRef.set( desiredState.operationalState() );
            return ReconcilerResponse.EMPTY;
        } );
    }
}
