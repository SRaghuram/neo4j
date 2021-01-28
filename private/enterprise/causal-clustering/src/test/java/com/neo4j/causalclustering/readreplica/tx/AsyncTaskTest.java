/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTaskTest
{
    @Test
    void shouldAbortIfToldTo()
    {
        AtomicBoolean executed = new AtomicBoolean();
        var asyncApplierTask = new AsyncTask( () ->
        {
            executed.set( true );
            return null;
        }, () -> true, new AsyncTaskEventHandler()
        {
            @Override
            public void onFailure( Exception e )
            {
                // do nothing
            }

            @Override
            public void onSuccess()
            {
                // do nothing
            }
        } );

        asyncApplierTask.run();

        assertThat( executed ).isFalse();
    }

    @Test
    void shouldRunAsExpected()
    {
        AtomicBoolean executed = new AtomicBoolean();
        AtomicReference<Exception> exception = new AtomicReference<>();
        var asyncApplierTask = new AsyncTask( () ->
        {
            executed.set( true );
            return null;
        }, () -> false, new AsyncTaskEventHandler()
        {
            @Override
            public void onFailure( Exception e )
            {
                exception.set( e );
            }

            @Override
            public void onSuccess()
            {

            }
        } );

        asyncApplierTask.run();

        assertThat( executed ).isTrue();
        assertThat( exception ).hasValue( null );
    }

    @Test
    void shouldTriggerFailureEventOnException()
    {
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();
        var failed = new RuntimeException( "Failed" );
        var asyncApplierTask = new AsyncTask( () ->
        {
            throw failed;
        }, () -> false, new AsyncTaskEventHandler()
        {
            @Override
            public void onFailure( Exception e )
            {
                exception.set( e );
            }

            @Override
            public void onSuccess()
            {
                success.set( true );
            }
        } );

        asyncApplierTask.run();

        assertThat( exception ).hasValue( failed );
        assertThat( success ).isFalse();
    }

    @Test
    void shouldTriggerOnSuccess()
    {
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();

        var asyncApplierTask = new AsyncTask( () ->
                null, () -> false, new AsyncTaskEventHandler()
        {
            @Override
            public void onFailure( Exception e )
            {
                exception.set( e );
            }

            @Override
            public void onSuccess()
            {
                success.set( true );
            }
        } );

        asyncApplierTask.run();
        assertThat( exception ).hasValue( null );
        assertThat( success ).isTrue();
    }
}
