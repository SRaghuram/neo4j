/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.iocontroller;

import org.junit.jupiter.api.Test;

import java.io.Flushable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
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
//        long stamp = INITIAL_STAMP;
//        repeatedlyCallMaybeLimitIO( limiter, stamp, 100 );

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
//        long stamp = INITIAL_STAMP;
//        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void mustNotRestrictIOToConfiguredRateWhenLimitIsDisabled()
    {
        createIOLimiter( 100 );

//        long stamp = INITIAL_STAMP;
//        limiter.disable();
//        try
//        {
//            stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
//            limiter.disable();
//            try
//            {
//                stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
//            }
//            finally
//            {
//                limiter.enable();
//            }
//            repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
//        }
//        finally
//        {
//            limiter.enable();
//        }

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

//        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
//        long stamp = INITIAL_STAMP;
//        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

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
//        long stamp = INITIAL_STAMP;
//        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
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
//        long stamp = INITIAL_STAMP;
//        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
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

//        limiter.maybeLimitIO( INITIAL_STAMP, 10, FLUSHABLE, flushOpportunity );
//        limiter.maybeLimitIO( INITIAL_STAMP, 20, FLUSHABLE, flushOpportunity );
//        limiter.maybeLimitIO( INITIAL_STAMP, 30, FLUSHABLE, flushOpportunity );
//        limiter.maybeLimitIO( INITIAL_STAMP, 3, FLUSHABLE, flushOpportunity );
//        limiter.maybeLimitIO( INITIAL_STAMP, 2, FLUSHABLE, flushOpportunity );
//        limiter.maybeLimitIO( INITIAL_STAMP, 1, FLUSHABLE, flushOpportunity );

        assertEquals( 66, pageCacheTracer.iopqPerformed() );
    }

    @Test
    void reportIOLimitsAndTimings()
    {
        createIOLimiter( 100 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        var flushEvent = pageCacheTracer.beginCacheFlush();
        var flushOpportunity = flushEvent.flushEventOpportunity();

//        long stamp = limiter.maybeLimitIO( INITIAL_STAMP, 1000, FLUSHABLE, flushOpportunity );
//        stamp = limiter.maybeLimitIO( stamp, 2000, FLUSHABLE, flushOpportunity );
//        clock.forward( Duration.ofSeconds( 1 ) );
//        stamp = limiter.maybeLimitIO( stamp, 7, FLUSHABLE, flushOpportunity );

        assertEquals( 2, pageCacheTracer.ioLimitedTimes() );
        assertEquals( 200, pageCacheTracer.ioLimitedMillis() );
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
//        repeatedlyCallMaybeLimitIO( limiter, INITIAL_STAMP, 1000000 );
        assertThat( pauseNanosCounter.get() ).isEqualTo( pauseTime );
    }

    private static long repeatedlyCallMaybeLimitIO( IOController ioController, long stamp, int iosPerIteration )
    {
        for ( int i = 0; i < 100; i++ )
        {
//            stamp = ioController.maybeLimitIO( stamp, iosPerIteration, FLUSHABLE, FlushEventOpportunity.NULL );
        }
        return stamp;
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
