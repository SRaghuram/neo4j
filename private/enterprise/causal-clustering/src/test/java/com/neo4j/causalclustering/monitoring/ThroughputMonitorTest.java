/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.monitoring.ThroughputMonitor.SAMPLING_WINDOW_DIVISOR;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class ThroughputMonitorTest
{
    private FakeClockJobScheduler scheduler = new FakeClockJobScheduler();

    private Duration samplingWindow = Duration.of( 5, SECONDS );
    private Duration samplingInterval = samplingWindow.dividedBy( SAMPLING_WINDOW_DIVISOR );
    private Duration halfSamplingWindow = samplingWindow.dividedBy( 2 );
    private Duration infinitesimal = Duration.of( 1, ChronoUnit.MILLIS );

    private ThroughputMonitor monitor;
    private MutableInt value = new MutableInt();
    private Supplier<Long> valueSupplier = () -> value.longValue();

    @BeforeEach
    void before()
    {
        monitor = new ThroughputMonitor( nullLogProvider(), scheduler, scheduler, samplingWindow, valueSupplier );
        monitor.start();
    }

    @AfterEach
    void after()
    {
        monitor.stop();
    }

    @Test
    void shouldNotHaveThroughputInitially()
    {
        Optional<Double> throughput = monitor.throughput();
        assertFalse( throughput.isPresent() );
    }

    @Test
    void shouldFailBeforeHalfOfTheSamplingWindow()
    {
        scheduler.forward( halfSamplingWindow.minus( infinitesimal ) );
        assertFalse( monitor.throughput().isPresent() );
    }

    @Test
    void shouldSucceedAfterHalfOfSamplingWindow()
    {
        scheduler.forward( halfSamplingWindow );
        assertTrue( monitor.throughput().isPresent() );
    }

    @Test
    void shouldFailAfterOneAndHalfOfTheSamplingWindow()
    {
        scheduler.forward( halfSamplingWindow.multipliedBy( 3 ).plus( infinitesimal ) );
        assertFalse( monitor.throughput().isPresent() );
    }

    @Test
    void shouldChooseBestEstimate()
    {
        // initial sample at start became: value 0 at 0 seconds

        value.setValue( 10 );
        scheduler.forward( samplingWindow.minus( samplingInterval ) ); // 4.875 seconds
        value.setValue( 30 );
        scheduler.forward( samplingInterval ); // "best" - exactly 5 seconds
        value.setValue( 60 );
        scheduler.forward( samplingInterval ); // 5.125 seconds

        value.setValue( 100 );
        scheduler.forward( samplingWindow.minus( samplingInterval ) ); // 10 seconds

        Optional<Double> throughput = monitor.throughput(); // 10 seconds
        assertTrue( throughput.isPresent() );
        assertEquals( (100 - 30d) / samplingWindow.getSeconds(), throughput.get(), 0.01 );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    void shouldAdaptToChangeInRate()
    {
        for ( int i = 0; i < 100; i++ )
        {
            scheduler.forward( samplingInterval );
        }

        assertEquals( 0d, monitor.throughput().get(), 0.01 );

        for ( int i = 0; i < 100; i++ )
        {
            value.add( 10 );
            scheduler.forward( samplingInterval );

            int valueDiff = Math.min( value.getValue(), 10 * SAMPLING_WINDOW_DIVISOR );
            double expectedThroughput = (double) valueDiff / samplingWindow.getSeconds();

            assertEquals( expectedThroughput, monitor.throughput().get(), 0.01 );
        }
    }
}
