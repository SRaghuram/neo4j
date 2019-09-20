/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.junit.jupiter.api.Test;

import org.neo4j.time.FakeClock;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CountdownTimerTest
{
    private FakeClock clock = new FakeClock();
    private CountdownTimer timer = new CountdownTimer( clock );

    @Test
    void shouldInitiallyBeExpired()
    {
        assertEquals( timer.timeToExpiry(), ZERO );
    }

    @Test
    void shouldUpdateExpiryTimeWhenReset()
    {
        timer.set( ofSeconds( 5 ) );
        assertEquals( timer.timeToExpiry(), ofSeconds( 5 ) );

        timer.set( ofSeconds( 10 ) );
        assertEquals( timer.timeToExpiry(), ofSeconds( 10 ) );
    }

    @Test
    void shouldUpdateExpiryTimeAsClockProgresses()
    {
        timer.set( ofSeconds( 5 ) );
        assertEquals( timer.timeToExpiry(), ofSeconds( 5 ) );

        clock.forward( ofSeconds( 3 ) );
        assertEquals( timer.timeToExpiry(), ofSeconds( 2 ) );

        clock.forward( ofSeconds( 2 ) );
        assertEquals( timer.timeToExpiry(), ZERO );

        clock.forward( ofSeconds( 1 ) );
        assertEquals( timer.timeToExpiry(), ZERO );
    }
}
