/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.FakeClock;

public class MaximumTotalTimeTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldFailWhenAllowedTimeHasPassed() throws StoreCopyFailedException
    {
        TimeUnit timeUnit = TimeUnit.SECONDS;
        FakeClock fakeClock = new FakeClock( 0, timeUnit );

        MaximumTotalTime maximumTotalTime = new MaximumTotalTime( 5, timeUnit, fakeClock );

        maximumTotalTime.assertContinue();
        fakeClock.forward( 5, timeUnit );
        maximumTotalTime.assertContinue();
        expectedException.expect( StoreCopyFailedException.class );
        fakeClock.forward( 1, timeUnit );
        maximumTotalTime.assertContinue();
    }
}
