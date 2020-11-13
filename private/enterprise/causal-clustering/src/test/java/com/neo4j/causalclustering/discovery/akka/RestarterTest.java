/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.neo4j.internal.helpers.TimeoutStrategy;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.constant;
import static org.neo4j.test.assertion.Assert.assertEventually;

class RestarterTest
{
    private final Supplier<Boolean> alwaysSucceed = () -> true;
    private final TimeoutStrategy constantTimeout = constant( 200, MILLISECONDS );

    @Test
    void shouldStartHealthy()
    {
        var restarter = new Restarter( constantTimeout, 0 );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeHealthyIfFirstRestartSucceeds()
    {
        var restarter = new Restarter( constantTimeout, 0 );

        restarter.restart( alwaysSucceed );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeHealthyIfSucceedsBeforeTooManyRestarts()
    {
        var restarter = new Restarter( constantTimeout, 5 );

        restarter.restart( succeedAfterN( 3 ) );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeUnhealthyAndThenHealthyAgainIfFailsAfterTooFewRestartsButThenSucceeds() throws InterruptedException
    {
        var restarter = new Restarter( constantTimeout, 5 );
        int failures = 8;
        var semaphore = new Semaphore( failures ); // Initialised with enough permits for restarter to get to unhealthy state, but no more

        runAsync( () -> restarter.restart( succeedAfterN( failures, semaphore ) ) );

        assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( false ) ), 20, TimeUnit.SECONDS );
        semaphore.release(); // Ensure restarter doesn't become unhealthy too briefly for test to detect state change
        assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( true ) ), 20, TimeUnit.SECONDS );
    }

    private Supplier<Boolean> succeedAfterN( int n )
    {
        return succeedAfterN( n, new Semaphore( n + 1 ) );
    }

    private Supplier<Boolean> succeedAfterN( int n, Semaphore semaphore )
    {
        AtomicInteger atomicInteger = new AtomicInteger( n );

        return () -> {
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
}
