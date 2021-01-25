/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ClusterInternalDbmsOperatorTest
{
    private OperatorConnector connector = mock( OperatorConnector.class );
    private ClusterInternalDbmsOperator operator = new ClusterInternalDbmsOperator( NullLogProvider.getInstance() );

    @BeforeEach
    void setup()
    {
        when( connector.trigger( any( ReconcilerRequest.class ) ) ).thenReturn( ReconcilerResult.EMPTY );
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
        var someDb = randomNamedDatabaseId();

        // when
        var stoppedDatabase = operator.stopForStoreCopy( someDb );

        // then
        verify( connector, times( 1 ) ).trigger( ReconcilerRequest.simple() );

        // when
        stoppedDatabase.release();

        // then
        verify( connector, times( 2 ) ).trigger( ReconcilerRequest.simple() );
    }

    @Test
    void shouldReturnIndependentContexts()
    {
        // given
        var databaseA = randomNamedDatabaseId();
        var databaseB = randomNamedDatabaseId();

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
        stopA1.release();

        // then
        assertEquals( STORE_COPYING, operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopA2.release();

        // then
        assertNull( operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopB1.release();

        // then
        assertNull( operatorState( databaseA ) );
        assertEquals( STORE_COPYING, operatorState( databaseB ) );

        // when
        stopB2.release();

        // then
        assertEquals( operator.desired(), Collections.emptyMap() );
    }

    @Test
    void shouldComplainWhenStartingTwice()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var stoppedDatabase = operator.stopForStoreCopy( someDb );
        stoppedDatabase.release();

        // when/then
        assertThrows( IllegalStateException.class, stoppedDatabase::release );
    }

    @Test
    void shouldNotDesireStoreCopyingWhileBootstrapping()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var bootstrapHandle = operator.bootstrap( someDb );

        // when
        var storeCopying = operator.stopForStoreCopy( someDb );

        // then
        assertNotEquals( STORE_COPYING, operatorState( someDb ) );

        // when
        bootstrapHandle.release();

        // then
        assertEquals( STORE_COPYING, operatorState( someDb ) );

        // when
        storeCopying.release();

        // then
        assertNull( operator.desired().get( someDb.name() ) );
    }

    private EnterpriseOperatorState operatorState( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( operator.desired().get( namedDatabaseId.name() ) )
                .map( EnterpriseDatabaseState::operatorState )
                .orElse( null );
    }

    @Test
    void shouldDesireStopForPanickedDatabase()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var desiredStateOnTrigger = new AtomicReference<EnterpriseOperatorState>();
        captureDesiredStateWhenOperatorTriggered( someDb, desiredStateOnTrigger );
        var e = new IllegalStateException();

        // when
        operator.stopOnPanic( someDb, e );

        // then
        assertEquals( STOPPED, desiredStateOnTrigger.get() );
        verify( connector ).trigger( ReconcilerRequest.panickedTarget( someDb, e ).build() );
    }

    @Test
    void shouldDesireStopForPanickedDatabaseWhileStoreCoping()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var desiredStateOnTrigger = new AtomicReference<EnterpriseOperatorState>();
        captureDesiredStateWhenOperatorTriggered( someDb, desiredStateOnTrigger );

        // when
        operator.stopForStoreCopy( someDb );
        operator.stopOnPanic( someDb, new IllegalStateException() );

        // then
        assertEquals( STOPPED, desiredStateOnTrigger.get() );
    }

    @Test
    void shouldNotTriggerRestartAfterStoreCopyWhenPanicked()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var e = new IllegalStateException();

        // when
        operator.stopOnPanic( someDb, e );
        var storeCopyHandle = operator.stopForStoreCopy( someDb );
        assertTrue( storeCopyHandle.release() );

        // then
        verify( connector ).trigger( ReconcilerRequest.panickedTarget( someDb, e ).build() );
    }

    private void captureDesiredStateWhenOperatorTriggered( NamedDatabaseId namedDatabaseId, AtomicReference<EnterpriseOperatorState> stateRef )
    {
        when( connector.trigger( any( ReconcilerRequest.class ) ) ).thenAnswer( invocation ->
        {
            var desiredState = operator.desired().get( namedDatabaseId.name() );
            stateRef.set( desiredState.operatorState() );
            return ReconcilerResult.EMPTY;
        } );
    }
}
