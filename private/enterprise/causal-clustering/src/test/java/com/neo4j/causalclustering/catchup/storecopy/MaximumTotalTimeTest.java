/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertThrows;

class MaximumTotalTimeTest
{
    @Test
    void shouldFailWhenAllowedTimeHasPassed() throws StoreCopyFailedException
    {
        var fiveSeconds = Duration.ofSeconds( 5 );
        var timeUnit = TimeUnit.SECONDS;
        var fakeClock = new FakeClock( 0, timeUnit );

        var maximumTotalTime = new MaximumTotalTime( fiveSeconds, fakeClock );

        maximumTotalTime.assertContinue();
        fakeClock.forward( 5, timeUnit );
        maximumTotalTime.assertContinue();
        fakeClock.forward( 1, timeUnit );
        assertThrows( StoreCopyFailedException.class, maximumTotalTime::assertContinue );
    }
}
