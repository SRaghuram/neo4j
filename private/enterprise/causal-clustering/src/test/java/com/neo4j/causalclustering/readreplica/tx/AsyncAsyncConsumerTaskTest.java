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

class AsyncAsyncConsumerTaskTest
{
    @Test
    void shouldAbortIfToldTo()
    {
        AtomicBoolean executed = new AtomicBoolean();
        var asyncApplierTask = new AsyncTask( () ->
        {
            executed.set( true );
            return null;
        }, () -> true, e ->
        {
            // do nothing
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
        }, () -> false, exception::set );

        asyncApplierTask.run();

        assertThat( executed ).isTrue();
        assertThat( exception ).hasValue( null );
    }

    @Test
    void shouldTriggerFailureEventOnException()
    {
        AtomicReference<Exception> exception = new AtomicReference<>();
        var failed = new RuntimeException( "Failed" );
        var asyncApplierTask = new AsyncTask( () ->
        {
            throw failed;
        }, () -> false, exception::set );

        asyncApplierTask.run();

        assertThat( exception ).hasValue( failed );
    }
}
