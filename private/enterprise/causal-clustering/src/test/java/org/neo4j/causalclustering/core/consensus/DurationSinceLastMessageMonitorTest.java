/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DurationSinceLastMessageMonitorTest
{
    private FakeClock clock = Clocks.fakeClock();
    private DurationSinceLastMessageMonitor subject = new DurationSinceLastMessageMonitor( clock );

    @Test
    void lastMessageTimestampTracked()
    {
        assertNull( subject.durationSinceLastMessage() );

        subject.timerReset();
        assertEquals( Duration.ofMillis( 0 ), subject.durationSinceLastMessage() );

        clock.forward( 1, TimeUnit.SECONDS );
        assertEquals( Duration.ofMillis( 1000 ), subject.durationSinceLastMessage() );
    }
}
