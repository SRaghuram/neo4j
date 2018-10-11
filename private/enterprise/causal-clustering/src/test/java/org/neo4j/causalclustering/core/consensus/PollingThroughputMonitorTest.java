/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PollingThroughputMonitorTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private FakeClock clock = Clocks.fakeClock();
    private CapturingJobScheduler capturingJobScheduler = new CapturingJobScheduler( clock );

    @Test
    void throughputIsEmptyWhenLessThanTwoDataPoints()
    {
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 5, Duration.ofSeconds( 1 ), capturingJobScheduler, () -> 5 );
        assertEquals( OptionalDouble.empty(), pollingThroughputMonitor.throughput() );
        capturingJobScheduler.getCapturedRunnableChangingClock().run();
        assertEquals( OptionalDouble.empty(), pollingThroughputMonitor.throughput() );
    }

    @Test
    void reportedPeriodMatchesExpected()
    {
        CapturingJobScheduler capturingJobScheduler = new CapturingJobScheduler( clock );
        new PollingThroughputMonitor( logProvider, clock, 123, Duration.ofSeconds( 3 ), capturingJobScheduler, () -> 1 );
        assertEquals( 3000, capturingJobScheduler.getPeriod() );
        assertEquals( TimeUnit.MILLISECONDS, capturingJobScheduler.getTimeUnit() );
    }

    @Test
    void emptyDurationsThrowError()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new PollingThroughputMonitor( logProvider, clock, 0, Duration.ofSeconds( 5 ), capturingJobScheduler, () -> 1 ) );
        assertThrows( IllegalArgumentException.class,
                () -> new PollingThroughputMonitor( logProvider, clock, 5, Duration.ofSeconds( 0 ), capturingJobScheduler, () -> 1 ) );
    }

    @Test
    void constantIncrementProducesConstantAverage()
    {
        AtomicInteger counter = new AtomicInteger( 1 );
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 300, Duration.ofSeconds( 1 ), capturingJobScheduler, counter::getAndIncrement );

        Runnable pollingProcess = capturingJobScheduler.getCapturedRunnableChangingClock();
        for ( int i = 0; i < 761; i++ )
        {
            pollingProcess.run();
        }
        assertEquals( OptionalDouble.of( 1.0 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void exceptionOnInvalidNumberOfMeasurements()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class,
                () -> new PollingThroughputMonitor( logProvider, clock, 0, Duration.ofSeconds( 2 ), capturingJobScheduler, () -> 1 ) );
        assertEquals( "There must be at least 2 measurements to report throughput. Actual = 0", exception.getMessage() );
        exception = assertThrows( IllegalArgumentException.class,
                () -> new PollingThroughputMonitor( logProvider, clock, 1, Duration.ofSeconds( 2 ), capturingJobScheduler, () -> 1 ) );
        assertEquals( "There must be at least 2 measurements to report throughput. Actual = 1", exception.getMessage() );
        new PollingThroughputMonitor( logProvider, clock, 2, Duration.ofSeconds( 2 ), capturingJobScheduler, () -> 1 );
    }

    @Test
    void negativeThroughputIsHandled()
    {
        AtomicInteger count = new AtomicInteger();
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 5, Duration.ofSeconds( 1 ), capturingJobScheduler, count::getAndDecrement );

        Runnable pollingProcess = capturingJobScheduler.getCapturedRunnableChangingClock();
        for ( int i = 0; i < 761; i++ )
        {
            pollingProcess.run();
        }
        assertEquals( OptionalDouble.of( -1.0 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void partialMeasurementsAreAccurate()
    {
        AtomicInteger atomicInteger = new AtomicInteger();
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 5, Duration.ofSeconds( 1 ), capturingJobScheduler, atomicInteger::getAndIncrement );
        Runnable pollingProcess = capturingJobScheduler.getCapturedRunnableChangingClock();

        for ( int i = 0; i < 3; i++ )
        {
            pollingProcess.run();
        }

        assertEquals( OptionalDouble.of( 1.0 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void negativeZeroHandled()
    {
        List<Long> values = Arrays.asList( 0L, -1L );
        AtomicLong index = new AtomicLong();
        LongSupplier negativeZeroThroughput = () ->
        {
            int boundedIndex = (int) Math.min( index.getAndIncrement(), values.size() - 1 );
            return values.get( boundedIndex );
        };

        int sizeOfArray = 5;
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, sizeOfArray, Duration.ofSeconds( 1 ), capturingJobScheduler, negativeZeroThroughput );

        capturingJobScheduler.getCapturedRunnableChangingClock().run();
        capturingJobScheduler.getCapturedRunnableChangingClock().run();
        double thrpt = pollingThroughputMonitor.throughput().getAsDouble();
        assertThat( thrpt, lessThanOrEqualTo( -0.0 ) );

        for ( int i = 0; i < sizeOfArray; i++ )
        {
            capturingJobScheduler.getCapturedRunnableChangingClock().run();
        }
        assertEquals( OptionalDouble.of( 0.0 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void pausesDuringMeasurementDoNotGetRecorded()
    {
        LongSupplier neverOnTimeSupplier = () ->
        {
            clock.forward( Duration.ofSeconds( 1 ) );
            return 3;
        };
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 4, Duration.ofSeconds( 1 ), capturingJobScheduler, neverOnTimeSupplier );

        for ( int i = 0; i < 100; i++ )
        {
            capturingJobScheduler.getCapturedRunnableChangingClock().run();
        }

        Assert.assertFalse( pollingThroughputMonitor.throughput().isPresent() );
    }

    @Test
    void ensureThroughputPerSecond()
    {
        FakeClock clock = Clocks.fakeClock();
        AtomicInteger counter = new AtomicInteger( 1 );
        CapturingJobScheduler jobScheduler = new CapturingJobScheduler( clock );
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( NullLogProvider.getInstance(), clock, 2, Duration.ofMinutes( 1 ), jobScheduler, counter::getAndIncrement );

        Runnable capturedRunnable = jobScheduler.getCapturedRunnableChangingClock();
        capturedRunnable.run();
        capturedRunnable.run();

        assertEquals( OptionalDouble.of( 1.0 / 60 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void avoidRoundingTimeToZero()
    {
        FakeClock clock = Clocks.fakeClock();
        AtomicInteger counter = new AtomicInteger( 1 );
        CapturingJobScheduler jobScheduler = new CapturingJobScheduler( clock );
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( NullLogProvider.getInstance(), clock, 2, Duration.ofMillis( 500 ), jobScheduler, counter::getAndIncrement );

        Runnable capturedRunnable = jobScheduler.getCapturedRunnableChangingClock();
        capturedRunnable.run();
        capturedRunnable.run();

        assertEquals( OptionalDouble.of( 2.0 ), pollingThroughputMonitor.throughput() );
    }

    @Test
    void avoidDivisionByZero()
    {
        FakeClock clock = Clocks.fakeClock();
        AtomicInteger counter = new AtomicInteger( 1 );
        CapturingJobScheduler jobScheduler = new CapturingJobScheduler( clock );
        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( NullLogProvider.getInstance(), clock, 2, Duration.ofSeconds( 1 ), jobScheduler, counter::getAndIncrement );

        Runnable capturedRunnable = jobScheduler.getCapturedRunnable();
        capturedRunnable.run();
        capturedRunnable.run();

        assertFalse( pollingThroughputMonitor.throughput().isPresent() );
    }

    @Test
    void pausesInSupplierArentRecorded()
    {
        FakeClock clock = Clocks.fakeClock();
        AtomicInteger counter = new AtomicInteger( 1 );
        CapturingJobScheduler jobScheduler = new CapturingJobScheduler( clock );

        LongSupplier delayingSupplier = () ->
        {
            long val = counter.getAndIncrement();
            if ( val % 2 == 0 )
            {
                clock.forward( Duration.ofSeconds( 2 ) );
            }
            return val;
        };

        PollingThroughputMonitor pollingThroughputMonitor =
                new PollingThroughputMonitor( logProvider, clock, 2, Duration.ofSeconds( 1 ), jobScheduler, delayingSupplier );

        jobScheduler.getCapturedRunnable().run();
        jobScheduler.getCapturedRunnable().run(); // Should not be recorded

        assertEquals( OptionalDouble.empty(), pollingThroughputMonitor.throughput() );
    }
}
