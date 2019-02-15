/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.FakeClock;

public class MaximumTotalTimeTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldFailWhenAllowedTimeHasPassed() throws StoreCopyFailedException
    {
        Duration fiveSeconds = Duration.ofSeconds( 5 );
        TimeUnit timeUnit = TimeUnit.SECONDS;
        FakeClock fakeClock = new FakeClock( 0, timeUnit );

        MaximumTotalTime maximumTotalTime = new MaximumTotalTime( fiveSeconds, fakeClock );

        maximumTotalTime.assertContinue();
        fakeClock.forward( 5, timeUnit );
        maximumTotalTime.assertContinue();
        expectedException.expect( StoreCopyFailedException.class );
        fakeClock.forward( 1, timeUnit );
        maximumTotalTime.assertContinue();
    }
}
