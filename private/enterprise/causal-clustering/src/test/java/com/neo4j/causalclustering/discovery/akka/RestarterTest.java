/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.test.assertion.Assert.assertEventually;

class RestarterTest
{
    private final Callable<Boolean> alwaysSucceed = () -> true;
    private final Callable<Boolean> alwaysFail = () -> false;
    private final RuntimeException testException = new RuntimeException( "Deliberate failure" );
    private final Callable<Boolean> alwaysThrow = () ->
    {
        throw testException;
    };

    @Test
    void shouldStartHealthy()
    {
        var restarter = ActorSystemRestarter.forTest( 0 );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeHealthyIfFirstRestartSucceeds() throws ActorSystemRestarter.RestartFailedException
    {
        var restartCounter = new AtomicInteger();
        var restarter = ActorSystemRestarter.forTest( 1 );

        restarter.restart( "test", countCalls( alwaysSucceed, restartCounter ) );

        assertThat( restarter.isHealthy(), is( true ) );
        assertThat( restartCounter.get(), is( 1 ) );
    }

    @Test
    void shouldBeUnhealthyIfCalledAndRestartsDisabled() throws ActorSystemRestarter.RestartFailedException
    {
        var restartCounter = new AtomicInteger();
        var restarter = ActorSystemRestarter.forTest( 0 );

        // we can pass null for the retry function here because it is never called
        assertThatThrownBy( () -> restarter.restart( "test", countCalls( alwaysSucceed, restartCounter ) ) )
                .isInstanceOf( ActorSystemRestarter.RestartFailedException.class );

        assertThat( restarter.isHealthy(), is( false ) );
        assertThat( restartCounter.get(), is( 0 ) );
    }

    @Test
    void shouldHandleIfRestartThrows() throws ActorSystemRestarter.RestartFailedException
    {
        var restartCounter = new AtomicInteger();
        var restarter = ActorSystemRestarter.forTest( 3 );

        // doesnt matter if we succeed
        assertThatThrownBy( () -> restarter.restart( "test", countCalls( alwaysThrow, restartCounter ) ) )
                .isInstanceOf( ActorSystemRestarter.RestartFailedException.class )
                .hasCause( testException );

        assertThat( restarter.isHealthy(), is( false ) );
        assertThat( restartCounter.get(), is( 3 ) );
    }

    @Test
    void shouldHandleIfRestartFails() throws ActorSystemRestarter.RestartFailedException
    {
        var restartCounter = new AtomicInteger();
        var restarter = ActorSystemRestarter.forTest( 3 );

        // doesnt matter if we succeed
        assertThatThrownBy( () -> restarter.restart( "test", countCalls( alwaysFail, restartCounter ) ) )
                .hasMessage( "Unable to restart test successfully after 3 attempts" )
                .isInstanceOf( ActorSystemRestarter.RestartFailedException.class );

        assertThat( restarter.isHealthy(), is( false ) );
        assertThat( restartCounter.get(), is( 3 ) );
    }

    @Test
    void shouldBeHealthyIfSucceedsBeforeTooManyRestarts() throws ActorSystemRestarter.RestartFailedException
    {
        var restarter = ActorSystemRestarter.forTest( 5 );

        restarter.restart( "test", succeedAfterN( 3 ) );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeUnhealthyAndThenHealthyAgainIfFailsThenSucceeds() throws InterruptedException, ActorSystemRestarter.RestartFailedException
    {
        var executor = Executors.newSingleThreadExecutor();
        try
        {
            int maxRetries = 9;

            var restarter = ActorSystemRestarter.forTest( maxRetries );
            var semaphore = new Semaphore( 1 );
            runAsync( () ->
                      {
                          try
                          {
                              restarter.restart( "test", succeedAfterN( maxRetries - 1, semaphore ) );
                          }
                          catch ( ActorSystemRestarter.RestartFailedException e )
                          {
                              throw new RuntimeException( e );
                          }
                      }
                    , executor );

            assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( false ) ), 20, TimeUnit.SECONDS );
            semaphore.release( maxRetries - 2 );

            // pause to allow the restarter thread to proceed
            Thread.sleep( 1000 );
            // still not healthy
            assertThat( restarter.isHealthy(), is( false ) );

            semaphore.release();
            assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( true ) ), 20, TimeUnit.SECONDS );
        }
        finally
        {
            executor.shutdownNow();
            assertThat( executor.awaitTermination( 1, MINUTES ), is( true ) );
        }
    }

    private Callable<Boolean> succeedAfterN( int n )
    {
        return succeedAfterN( n, new Semaphore( n + 1 ) );
    }

    private Callable<Boolean> succeedAfterN( int n, Semaphore semaphore )
    {
        AtomicInteger atomicInteger = new AtomicInteger( n );

        return () ->
        {
            try
            {
                semaphore.acquire();
                return atomicInteger.decrementAndGet() < 0;
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Callable<Boolean> countCalls( Callable<Boolean> fn, AtomicInteger counter )
    {
        return () ->
        {
            counter.incrementAndGet();
            return fn.call();
        };
    }
}
