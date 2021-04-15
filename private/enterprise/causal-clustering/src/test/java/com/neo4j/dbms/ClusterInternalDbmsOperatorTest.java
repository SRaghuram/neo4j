/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ClusterInternalDbmsOperatorTest
{
    private final DbmsReconciler reconciler = mock( DbmsReconciler.class );
    private final TestOperatorConnector connector = new TestOperatorConnector( reconciler );
    private final ClusterInternalDbmsOperator operator = new ClusterInternalDbmsOperator( NullLogProvider.getInstance() );

    @BeforeEach
    void setup()
    {
        when( reconciler.reconcile( anyCollection(), any( ReconcilerRequest.class ) ) ).thenReturn( ReconcilerResult.EMPTY );
        connector.setOperators( List.of( operator ) );
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
        verify( reconciler, times( 1 ) ).reconcile( anyCollection(), any( ReconcilerRequest.class ) );

        // when
        stoppedDatabase.release();

        // then
        verify( reconciler, times( 2 ) ).reconcile( anyCollection(), any( ReconcilerRequest.class ) );
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
    void shouldThrowWhenFailsToPutDatabaseInStoreCopying()
    {
        // given
        var e = new RuntimeException();
        var someDb = randomNamedDatabaseId();
        var actualState = new EnterpriseDatabaseState( someDb, STARTED );
        var desiredState = new EnterpriseDatabaseState( someDb, STORE_COPYING );
        var stepResult = new ReconcilerStepResult( actualState, e, desiredState );
        var result = new ReconcilerResult( Map.of( someDb.name(), CompletableFuture.completedFuture( stepResult ) ) );
        when( reconciler.reconcile( anyCollection(), any( ReconcilerRequest.class ) ) ).thenReturn( result );

        // when
        assertThrows( RuntimeException.class, () -> operator.stopForStoreCopy( someDb ) );
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
        var e = new IllegalStateException();
        var expectedDesired = Map.of( someDb.name(), new EnterpriseDatabaseState( someDb, STOPPED ) );
        var expectedRequest = ReconcilerRequest.panickedTarget( someDb, e ).build();
        var expectedTriggerCall = Pair.of( expectedDesired, expectedRequest );

        // when
        operator.stopOnPanic( someDb, e );
        var triggerCalls = connector.triggerCalls();

        // then
        assertThat( triggerCalls ).hasSize( 1 );
        assertThat( triggerCalls ).contains( expectedTriggerCall );
    }

    @Test
    void shouldDesireStopForPanickedDatabaseWhileStoreCoping()
    {
        // given
        var someDb = randomNamedDatabaseId();
        var e = new IllegalStateException();
        var panicDesired = Map.of( someDb.name(), new EnterpriseDatabaseState( someDb, STOPPED ) );
        var panicRequest = ReconcilerRequest.panickedTarget( someDb, e ).build();
        var panicTriggerCall = Pair.of( panicDesired, panicRequest );
        var storeCopyDesired = Map.of( someDb.name(), new EnterpriseDatabaseState( someDb, STORE_COPYING ) );
        var storeCopyTriggerCall = Pair.of( storeCopyDesired, ReconcilerRequest.simple() );

        // when
        operator.stopForStoreCopy( someDb );
        operator.stopOnPanic( someDb, e );

        var triggerCalls = connector.triggerCalls();

        // then
        assertThat( triggerCalls ).hasSize( 2 );
        assertThat( triggerCalls ).containsExactly( storeCopyTriggerCall, panicTriggerCall );
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
        verify( reconciler, atMostOnce() ).reconcile( anyCollection(), eq( ReconcilerRequest.panickedTarget( someDb, e ).build() ) );
    }

}
