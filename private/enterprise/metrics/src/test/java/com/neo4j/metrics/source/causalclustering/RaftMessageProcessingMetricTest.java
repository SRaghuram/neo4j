/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.SlidingWindowReservoir;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftMessageProcessingMetricTest
{
    private final RaftMessageProcessingMetric metric = RaftMessageProcessingMetric.createUsing( () -> new SlidingWindowReservoir( 1000 ) );

    @Test
    void shouldDefaultAllMessageTypesToEmptyTimer()
    {
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            assertEquals( 0, metric.timer( type ).getCount() );
        }
        assertEquals( 0, metric.timer().getCount() );
    }

    @Test
    void shouldBeAbleToUpdateAllMessageTypes()
    {
        // given
        int durationNanos = 5;
        double delta = 0.002;

        // when
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            metric.updateTimer( type, Duration.ofNanos( durationNanos ) );
            assertEquals( 1, metric.timer( type ).getCount() );
            assertEquals( durationNanos, metric.timer( type ).getSnapshot().getMean(), delta );
        }

        // then
        assertEquals( RaftMessages.Type.values().length, metric.timer().getCount() );
        assertEquals( durationNanos, metric.timer().getSnapshot().getMean(), delta );
    }

    @Test
    void shouldDefaultDelayToZero()
    {
        assertEquals( 0, metric.delay() );
    }

    @Test
    void shouldUpdateDelay()
    {
        metric.setDelay( Duration.ofMillis( 5 ) );
        assertEquals( 5, metric.delay() );
    }
}
