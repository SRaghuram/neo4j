/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class ThroughputMonitorTest
{
    private static final int START_VALUE = 0;
    private final Duration warnThreshold = Duration.ofSeconds( 1 );
    private final Duration samplingWindow = Duration.ofSeconds( 5 );
    private final int samples = 5;
    private final MutableInt value = new MutableInt( START_VALUE );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final FakeClock clock = Clocks.fakeClock();

    ThroughputMonitorService throughputMonitorService = mock( ThroughputMonitorService.class );

    private ThroughputMonitor monitor =
            new ThroughputMonitor( logProvider, clock, warnThreshold, samplingWindow, samplingWindow.dividedBy( 2 ),
                                   new QualitySampler<>( clock, Duration.ofDays( 1 ), value::longValue ), samples,
                                   throughputMonitorService );

    @Test
    void shouldNotHaveThroughputInitially()
    {
        assertNoThroughput();
    }

    @Test
    void shouldRegisterOnStart()
    {
        monitor.start();

        verify( throughputMonitorService, times( 1 ) ).registerMonitor( monitor );
    }

    @Test
    void shouldUnregisterOnStop()
    {
        monitor.stop();

        verify( throughputMonitorService, times( 1 ) ).unregisterMonitor( monitor );
    }

    @Test
    void stoppedThroughputMonitorShouldNotGiveThroughput()
    {
        monitor.start();
        fillBuffer();
        assertThat( monitor.throughput() ).isNotEmpty();
        monitor.stop();
        assertThat( monitor.throughput() ).isEmpty();
    }

    @Test
    void shouldNotSampleIfNotStarted()
    {
        for ( int i = 0; i < samples; i++ )
        {
            monitor.samplingTask();
        }
        assertNoThroughput();
    }

    @Test
    void shouldNotProvideThroughputIfLastSampleIsOld()
    {
        monitor.start();

        // capped logger is set to 1 min
        clock.forward( 1, MINUTES );

        for ( int i = 0; i < samples; i++ )
        {
            monitor.samplingTask();
            value.increment();
            clock.forward( 1, SECONDS );
        }

        clock.forward( 1, SECONDS );

        var throughput = monitor.throughput();

        assertNoThroughput();
        assertThat( logProvider ).forClass( ThroughputMonitor.class ).forLevel( WARN )
                .containsMessages( "Last measurement was made 2000 ms ago" );
    }

    @Test
    void requireFullSetOfSamples()
    {
        monitor.start();

        for ( int i = 0; i < samples - 1; i++ )
        {
            assertNoThroughput();
            monitor.samplingTask();
            value.increment();
            clock.forward( 1, SECONDS );
        }
        monitor.samplingTask();

        var expectedThroughput = (value.getValue() - START_VALUE) * 1000.0 / clock.millis();

        assertEquals( expectedThroughput, monitor.throughput().get() );
    }

    @Test
    void shouldNotAcceptToSmallWindow()
    {
        monitor.start();

        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        clock.forward( 2, SECONDS );
        value.increment();
        monitor.samplingTask();

        var throughput = monitor.throughput();
        assertThat( throughput ).isEmpty();
    }

    @Test
    void shouldNotAcceptTooBigWindow()
    {
        monitor.start();

        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        clock.forward( 8, SECONDS );
        value.increment();
        monitor.samplingTask();

        var throughput = monitor.throughput();
        assertThat( throughput ).isEmpty();
    }

    @Test
    void shouldAcceptSightlyBigWindow()
    {
        monitor.start();

        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        clock.forward( 7, SECONDS );
        value.increment();
        monitor.samplingTask();

        var throughput = monitor.throughput();
        assertThat( throughput ).isNotEmpty();
    }

    @Test
    void shouldAcceptSightlySmallWindow()
    {
        monitor.start();

        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        monitor.samplingTask();
        clock.forward( 3, SECONDS );
        value.increment();
        monitor.samplingTask();

        var throughput = monitor.throughput();
        assertThat( throughput ).isNotEmpty();
    }

    @Test
    void shouldCalculateCorrectThroughputDespiteIrregularMeasurements()
    {
        monitor.start();

        monitor.samplingTask();
        clock.forward( 10, SECONDS );
        value.increment();
        monitor.samplingTask();
        clock.forward( 1, SECONDS );
        value.increment();
        monitor.samplingTask();
        clock.forward( 1, SECONDS );
        value.increment();
        monitor.samplingTask();
        clock.forward( 1, SECONDS );
        value.increment();
        monitor.samplingTask();

        var expectedThroughput = (value.getValue() - 1) * 1000.0 / 3_000;

        assertEquals( expectedThroughput, monitor.throughput().get() );
    }

    private void fillBuffer()
    {
        for ( int i = 0; i < samples; i++ )
        {
            monitor.samplingTask();
            clock.forward( 1, SECONDS );
            value.increment();
        }
    }

    private void assertNoThroughput()
    {
        assertThat( monitor.throughput() ).isEmpty();
    }
}
