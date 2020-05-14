/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.test.assertion.Assert.assertEventually;

class RestarterTest
{
    private final Supplier<Boolean> alwaysSucceed = () -> true;
    private final TimeoutStrategy constantTimeout = new ConstantTimeTimeoutStrategy( 200, MILLISECONDS );

    @Test
    void shouldStartHealthy()
    {
        Restarter restarter = new Restarter( constantTimeout, 0 );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeHealthyIfFirstRestartSucceeds()
    {
        Restarter restarter = new Restarter( constantTimeout, 0 );

        restarter.restart( alwaysSucceed );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldBeHealthyIfSucceedsBeforeTooManyRestarts()
    {
        Restarter restarter = new Restarter( constantTimeout, 5 );

        restarter.restart( succeedAfterN( 3 ) );

        assertThat( restarter.isHealthy(), is( true ) );
    }

    @Test
    void shouldNotBeHealthyIfStillFailsAfterTooManyRestarts() throws InterruptedException
    {
        Restarter restarter = new Restarter( constantTimeout, 5 );

        runAsync( () -> restarter.restart( succeedAfterN( 8 ) ) );

        assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( false ) ), 20, TimeUnit.SECONDS );
    }

    @Test
    void shouldBeUnhealthyAndThenHealthyAgainIfFailsAfterTooFewRestartsButThenSucceeds() throws InterruptedException
    {
        Restarter restarter = new Restarter( constantTimeout, 5 );

        runAsync( () -> restarter.restart( succeedAfterN( 8 ) ) );

        assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( false ) ), 20, TimeUnit.SECONDS );
        assertEventually( restarter::isHealthy, new HamcrestCondition<>( is( true ) ), 20, TimeUnit.SECONDS );
    }

    private Supplier<Boolean> succeedAfterN( int n )
    {
        AtomicInteger atomicInteger = new AtomicInteger( n );

        return () -> atomicInteger.decrementAndGet() < 0;
    }
}
