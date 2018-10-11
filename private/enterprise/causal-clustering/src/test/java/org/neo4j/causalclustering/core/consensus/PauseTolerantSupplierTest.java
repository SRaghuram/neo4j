/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.LongSupplier;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PauseTolerantSupplierTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private FakeClock clock = new FakeClock();
    private LongSupplier valueSupplier = () -> 1;

    @Test
    void validWhenNoPause()
    {
        PauseTolerantSupplier subject = new PauseTolerantSupplier( logProvider, clock, valueSupplier );

        TimestampedValue value = subject.get().get();

        assertEquals( 1, value.getValue() );
    }

    @Test
    void emptyWhenPauseDetected()
    {
        LongSupplier valueSupplier = () ->
        {
            clock.forward( Duration.ofSeconds( 5 ) );
            return 1;
        };
        PauseTolerantSupplier subject = new PauseTolerantSupplier( logProvider, clock, valueSupplier );

        assertFalse( subject.get().isPresent() );
    }
}
