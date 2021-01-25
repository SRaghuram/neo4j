/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.jupiter.api.Test;

import java.io.Flushable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurableIOLimiterTest
{
    private ConfigurableIOLimiter limiter;
    private Config config;
    private AtomicLong pauseNanosCounter;
    private static final Flushable FLUSHABLE = () -> {};

    @Test
    void mustPutDefaultLimitOnIOWhenNoLimitIsConfigured()
    {
        createIOLimiter( Config.defaults() );

        // Do 100*100 = 10000 IOs real quick, when we're limited to 1000 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 100 );

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
        limiter.disableLimit();
        try
        {
            assertUnlimited();
            limiter.disableLimit();
            try
            {
                assertUnlimited();
            }
            finally
            {
                limiter.enableLimit();
            }
        }
        finally
        {
            limiter.enableLimit();
        }
    }

    @Test
    void mustRestrictIORateToConfiguredLimit()
    {
        createIOLimiter( 100 );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void mustNotRestrictIOToConfiguredRateWhenLimitIsDisabled()
    {
        createIOLimiter( 100 );

        long stamp = IOLimiter.INITIAL_STAMP;
        limiter.disableLimit();
        try
        {
            stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
            limiter.disableLimit();
            try
            {
                stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
            }
            finally
            {
                limiter.enableLimit();
            }
            repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        }
        finally
        {
            limiter.enableLimit();
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
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

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
        limiter.disableLimit();
        // ...while a dynamic configuration update happens
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, 100, getClass().getSimpleName()  );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enableLimit();
        // ...must make the limiter limit.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void dynamicConfigurationUpdateDisablingLimiterMustNotDisableLimiter()
    {
        // Create initially limited
        createIOLimiter( 100 );
        // Disable the limiter...
        limiter.disableLimit();
        // ...while a dynamic configuration update happens
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, -1, getClass().getSimpleName()  );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enableLimit();
        // ...must maintain the limiter disabled.
        assertUnlimited();
        // Until it is re-enabled.
        config.setDynamic( GraphDatabaseSettings.check_point_iops_limit, 100, getClass().getSimpleName()  );
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        assertThat( pauseNanosCounter.get() ).isGreaterThan( TimeUnit.SECONDS.toNanos( 9 ) );
    }

    @Test
    void configuredLimitMustReflectCurrentState()
    {
        createIOLimiter( 100 );

        assertThat( limiter.isLimited() ).isEqualTo( true );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited() ).isEqualTo( true );
    }

    @Test
    void configuredDisabledLimitShouldBeUnlimited()
    {
        createIOLimiter( -1 );

        assertThat( limiter.isLimited() ).isEqualTo( false );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited() ).isEqualTo( false );
    }

    @Test
    void unlimitedShouldAlwaysBeUnlimited()
    {
        IOLimiter limiter = IOLimiter.UNLIMITED;

        assertThat( limiter.isLimited() ).isEqualTo( false );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited() ).isEqualTo( false );

        limiter.enableLimit();
        try
        {
            assertThat( limiter.isLimited() ).isEqualTo( false );
        }
        finally
        {
            limiter.disableLimit();
        }
    }

    private void createIOLimiter( Config config )
    {
        this.config = config;
        pauseNanosCounter = new AtomicLong();
        ObjLongConsumer<Object> pauseNanos = ( blocker, nanos ) -> pauseNanosCounter.getAndAdd( nanos );
        limiter = new ConfigurableIOLimiter( config, pauseNanos );
    }

    private void createIOLimiter( int limit )
    {
        createIOLimiter( Config.defaults( GraphDatabaseSettings.check_point_iops_limit, limit ) );
    }

    private void assertUnlimited()
    {
        long pauseTime = pauseNanosCounter.get();
        repeatedlyCallMaybeLimitIO( limiter, IOLimiter.INITIAL_STAMP, 1000000 );
        assertThat( pauseNanosCounter.get() ).isEqualTo( pauseTime );
    }

    private long repeatedlyCallMaybeLimitIO( IOLimiter ioLimiter, long stamp, int iosPerIteration )
    {
        for ( int i = 0; i < 100; i++ )
        {
            stamp = ioLimiter.maybeLimitIO( stamp, iosPerIteration, FLUSHABLE );
        }
        return stamp;
    }

    private static void multipleDisableShouldReportUnlimited( IOLimiter limiter )
    {
        limiter.disableLimit();
        try
        {
            assertThat( limiter.isLimited() ).isEqualTo( false );
            limiter.disableLimit();
            try
            {
                assertThat( limiter.isLimited() ).isEqualTo( false );
            }
            finally
            {
                limiter.enableLimit();
            }
            assertThat( limiter.isLimited() ).isEqualTo( false );
        }
        finally
        {
            limiter.enableLimit();
        }
    }
}
