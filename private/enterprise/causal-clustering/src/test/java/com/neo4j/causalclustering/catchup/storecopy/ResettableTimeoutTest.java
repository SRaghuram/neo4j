/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResettableTimeoutTest
{
    @Test
    void shouldTimeout()
    {
        Duration timeout = Duration.ofMillis( 1 );
        FakeClock fakeClock = Clocks.fakeClock();
        ResettableTimeout resettableTimeout = new ResettableTimeout( timeout, fakeClock );

        assertTrue( resettableTimeout.canContinue() );

        fakeClock.forward( 2, TimeUnit.MILLISECONDS );

        assertFalse( resettableTimeout.canContinue() );
    }

    @Test
    void shouldNotTimeoutIfResetBeforeContinueCheck()
    {
        Duration timeout = Duration.ofMillis( 1 );
        FakeClock fakeClock = Clocks.fakeClock();
        ResettableTimeout resettableTimeout = new ResettableTimeout( timeout, fakeClock );

        assertTrue( resettableTimeout.canContinue() );

        fakeClock.forward( 2, TimeUnit.MILLISECONDS );

        resettableTimeout.reset();

        assertTrue( resettableTimeout.canContinue() );
    }
}
