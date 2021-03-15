/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.iocontroller;

import org.junit.jupiter.api.Test;

import java.io.Flushable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigurableIOControllerTest
{
    private ConfigurableIOController limiter;
    private FakeClock clock;
    private Config config;
    private AtomicLong pauseNanosCounter;
    private static final Flushable FLUSHABLE = () -> {};

    @Test
    void mustPutDefaultLimitOnIOWhenNoLimitIsConfigured()
    {
        createIOLimiter( Config.defaults() );

        // Do 100*100 = 10000 IOs real quick, when we're limited to 1000 IOPS.
        repeatedlyCallMaybeLimitIO( limiter, 100 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void mustNotPutLimitOnIOWhenConfiguredToBeUnlimited()
    {
        createIOLimiter( -1 );
        assertUnlimited();
    }

    @Test
    void mustNotPutLimitOnIOWhenLimitingIsDisabledAndNoLimitIsConfigured()
    {
        createIOLimiter( Config.defaults() );
        limiter.disable();
        try
        {
            assertUnlimited();
            limiter.disable();
            try
            {
                assertUnlimited();
            }
            finally
            {
                limiter.enable();
            }
        }
        finally
        {
            limiter.enable();
        }
    }

    @Test
    void mustRestrictIORateToConfiguredLimit()
    {
        createIOLimiter( 100 );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        repeatedlyCallMaybeLimitIO( limiter, 10 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void mustNotRestrictIOToConfiguredRateWhenLimitIsDisabled()
    {
        createIOLimiter( 100 );

        limiter.disable();
        try
        {
            repeatedlyCallMaybeLimitIO( limiter, 10 );
            limiter.disable();
            try
            {
                repeatedlyCallMaybeLimitIO( limiter, 10 );
            }
            finally
            {
                limiter.enable();
            }
            repeatedlyCallMaybeLimitIO( limiter, 10 );
        }
        finally
        {
            limiter.enable();
        }

        // We should've spent no time rushing
        assertThat( pauseNanosCounter.get() ).isEqualTo( 0L );
    }

    @Test
    void dynamicConfigurationUpdateMustBecomeVisible()
    {
        // Create initially unlimited
        createIOLimiter( 0 );

        // Then set a limit of 100 IOPS
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, 100, getClass().getSimpleName() );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        repeatedlyCallMaybeLimitIO( limiter, 10 );

        // Then assert that the updated limit is respected
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );

        // Change back to unlimited
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, -1, getClass().getSimpleName()  );

        // And verify it's respected
        assertUnlimited();
    }

    @Test
    void dynamicConfigurationUpdateEnablingLimiterMustNotDisableLimiter()
    {
        // Create initially unlimited
        createIOLimiter( 0 );
        // Disable the limiter...
        limiter.disable();
        // ...while a dynamic configuration update happens
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, 100, getClass().getSimpleName()  );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enable();
        // ...must make the limiter limit.
        repeatedlyCallMaybeLimitIO( limiter, 10 );
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void dynamicConfigurationUpdateDisablingLimiterMustNotDisableLimiter()
    {
        // Create initially limited
        createIOLimiter( 100 );
        // Disable the limiter...
        limiter.disable();
        // ...while a dynamic configuration update happens
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, -1, getClass().getSimpleName()  );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enable();
        // ...must maintain the limiter disabled.
        assertUnlimited();
        // Until it is re-enabled.
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, 100, getClass().getSimpleName()  );
        repeatedlyCallMaybeLimitIO( limiter, 10 );
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void configuredLimitMustReflectCurrentState()
    {
        createIOLimiter( 100 );

        assertThat( limiter.isEnabled() ).isEqualTo( true );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isEnabled() ).isEqualTo( true );
    }

    @Test
    void configuredDisabledLimitShouldBeUnlimited()
    {
        createIOLimiter( -1 );

        assertThat( limiter.isEnabled() ).isEqualTo( false );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isEnabled() ).isEqualTo( false );
    }

    @Test
    void unlimitedShouldAlwaysBeUnlimited()
    {
        IOController limiter = IOController.DISABLED;

        assertThat( limiter.isEnabled() ).isEqualTo( false );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isEnabled() ).isEqualTo( false );

        limiter.enable();
        try
        {
            assertThat( limiter.isEnabled() ).isEqualTo( false );
        }
        finally
        {
            limiter.disable();
        }
    }

    @Test
    void reportRecentlyCompletedIOOperations()
    {
        createIOLimiter( -1 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

        limiter.maybeLimitIO( 10, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 20, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 30, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 3, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 2, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 1, FLUSHABLE, flushOpportunity );

        assertEquals( 66, pageCacheTracer.iopqPerformed() );
    }

    @Test
    void reportIOLimitsAndTimings()
    {
        createIOLimiter( 100 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

        limiter.maybeLimitIO( 1000, FLUSHABLE, flushOpportunity );
        limiter.maybeLimitIO( 2000, FLUSHABLE, flushOpportunity );
        clock.forward( Duration.ofSeconds( 1 ) );
        limiter.maybeLimitIO( 7, FLUSHABLE, flushOpportunity );

        assertEquals( 2, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 200, pageCacheTracer.ioLimitedMillis() );
    }

    @Test
    void resetReportedExternalIoOnQuantumElapse()
    {
        createIOLimiter( 100 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

        limiter.reportIO( 400 );
        clock.forward( 1, TimeUnit.SECONDS );

        limiter.maybeLimitIO( 1, FLUSHABLE, flushOpportunity );

        assertEquals( 0, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 0, limiter.getExternalIO() );
    }

    @Test
    void considerExternalIODuringMaybeLimitEvaluation()
    {
        createIOLimiter( 100 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

        limiter.reportIO( 400 );
        limiter.maybeLimitIO( 1, FLUSHABLE, flushOpportunity );

        assertEquals( 1, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 0, limiter.getExternalIO() );
    }

    @Test
    void resetExternalIODuringMaybeLimitEvaluation()
    {
        createIOLimiter( 1000 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

        limiter.reportIO( 98 );
        limiter.maybeLimitIO( 1, FLUSHABLE, flushOpportunity );

        assertEquals( 0, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 0, limiter.getExternalIO() );

        limiter.reportIO( 99 );
        limiter.maybeLimitIO( 1, FLUSHABLE, flushOpportunity );

        assertEquals( 1, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 0, limiter.getExternalIO() );
    }

    @Test
    void sumReportedExternalIOToController()
    {
        createIOLimiter( 100 );

        limiter.reportIO( 1 );
        limiter.reportIO( 2 );
        limiter.reportIO( 3 );
        limiter.reportIO( 4 );

        assertEquals( 10, limiter.getExternalIO() );
    }

    @Test
    void resetReportedExternalIoOnEnableDisable()
    {
        createIOLimiter( 100 );

        limiter.reportIO( 10 );
        limiter.reportIO( 20 );
        assertEquals( 30, limiter.getExternalIO() );

        limiter.disable();
        limiter.enable();

        assertEquals( 0, limiter.getExternalIO() );
    }

    private void createIOLimiter( Config config )
    {
        this.config = config;
        pauseNanosCounter = new AtomicLong();
        ObjLongConsumer<Object> pauseNanos = ( blocker, nanos ) -> pauseNanosCounter.getAndAdd( nanos );
        clock = new FakeClock();
        limiter = new ConfigurableIOController( config, pauseNanos, clock );
    }

    private void createIOLimiter( int limit )
    {
        createIOLimiter( Config.defaults( GraphDatabaseSettings.check_point_iops_limit, limit ) );
    }

    private void assertUnlimited()
    {
        long pauseTime = pauseNanosCounter.get();
        repeatedlyCallMaybeLimitIO( limiter, 1000000 );
        assertThat( pauseNanosCounter.get() ).isEqualTo( pauseTime );
    }

    private static void repeatedlyCallMaybeLimitIO( IOController ioController, int iosPerIteration )
    {
        for ( int i = 0; i < 100; i++ )
        {
            ioController.maybeLimitIO( iosPerIteration, FLUSHABLE, FlushEventOpportunity.NULL );
        }
    }

    private static void multipleDisableShouldReportUnlimited( IOController limiter )
    {
        limiter.disable();
        try
        {
            assertThat( limiter.isEnabled() ).isEqualTo( false );
            limiter.disable();
            try
            {
                assertThat( limiter.isEnabled() ).isEqualTo( false );
            }
            finally
            {
                limiter.enable();
            }
            assertThat( limiter.isEnabled() ).isEqualTo( false );
        }
        finally
        {
            limiter.enable();
        }
    }
}
