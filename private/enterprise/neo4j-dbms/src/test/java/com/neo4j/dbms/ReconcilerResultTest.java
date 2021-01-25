/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ReconcilerResultTest
{

    @Test
    void shouldNotThrowWhenAwaiting()
    {
        // given
        var failure = new DatabaseManagementException( "Failed to Start" );
        var fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var barId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var fooResult = failedReconcilerJob( fooId, failure );
        var barResult = successfulReconcilerJob( barId );

        var jobs = Map.of( "foo", fooResult, "bar", barResult );
        var reconcilerResult = new ReconcilerResult( jobs );

        // when
        reconcilerResult.awaitAll();
        reconcilerResult.await( fooId );
        reconcilerResult.await( List.of( fooId, barId ) );
        //If none of these throw we succeed
    }

    @Test
    void shouldCompleteImmediatelyWithEmpty() throws InterruptedException
    {
        // given
        var result = new ReconcilerResult( Map.of() );
        var waitingFinished = new CountDownLatch( 1 );

        // when
        CompletableFuture.runAsync( () ->
        {
            result.awaitAll();
            waitingFinished.countDown();
        } );

        // then
        waitingFinished.await( 10, TimeUnit.SECONDS );
    }

    @Test
    void shouldThrowWhenJoining()
    {
        // given
        var failure = new DatabaseManagementException( "Failed to Start" );
        var fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var barId = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var fooResult = failedReconcilerJob( fooId, failure );
        var barResult = successfulReconcilerJob( barId );

        var jobs = Map.of( "foo", fooResult, "bar", barResult );
        var reconcilerResult = new ReconcilerResult( jobs );

        // when / then
        assertThrows( DatabaseManagementException.class, reconcilerResult::joinAll );
        assertThrows( DatabaseManagementException.class, () -> reconcilerResult.join( fooId ) );
        assertThrows( DatabaseManagementException.class, () -> reconcilerResult.join( List.of( fooId, barId ) ) );
    }

    @Test
    void shouldSuppressAllErrorsIfMultipleThrown()
    {
        // given
        var fooFailure = new DatabaseManagementException( "Failed to start foo" );
        var barFailure = new DatabaseManagementException( "Failed to start bar" );
        var fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var barId = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var fooResult = failedReconcilerJob( fooId, fooFailure );
        var barResult = failedReconcilerJob( barId, barFailure );

        var jobs = Map.of( "foo", fooResult, "bar", barResult );
        var reconcilerResult = new ReconcilerResult( jobs );

        // when / then
        try
        {
            reconcilerResult.joinAll();
            fail();
        }
        catch ( DatabaseManagementException e )
        {
            assertTrue( Exceptions.findCauseOrSuppressed( e, Predicate.isEqual( fooFailure ) ).isPresent() );
            assertTrue( Exceptions.findCauseOrSuppressed( e, Predicate.isEqual( barFailure ) ).isPresent() );
        }
    }

    private CompletableFuture<ReconcilerStepResult> failedReconcilerJob( NamedDatabaseId databaseId, DatabaseManagementException failure )
    {
        var currentState = new EnterpriseDatabaseState( databaseId, STOPPED );
        var desiredState = new EnterpriseDatabaseState( databaseId, STARTED );
        var result = new ReconcilerStepResult( currentState.failed( failure ), failure, desiredState );
        return CompletableFuture.completedFuture( result );
    }

    private CompletableFuture<ReconcilerStepResult> successfulReconcilerJob( NamedDatabaseId databaseId )
    {
        var state = new EnterpriseDatabaseState( databaseId, STARTED );
        var result = new ReconcilerStepResult( state, null, state ); // current and desired are the same
        return CompletableFuture.completedFuture( result );
    }
}
