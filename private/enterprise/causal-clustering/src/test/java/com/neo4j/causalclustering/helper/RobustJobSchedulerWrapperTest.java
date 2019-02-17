/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( LifeExtension.class )
class RobustJobSchedulerWrapperTest
{
    private final int DEFAULT_TIMEOUT_MS = 5000;

    @Inject
    private LifeSupport schedulerLife;
    private final JobScheduler actualScheduler = createInitialisedScheduler();

    private final Log log = mock( Log.class );

    @BeforeEach
    void setup()
    {
        schedulerLife.add( actualScheduler );
    }

    @Test
    void oneOffJobWithExceptionShouldLog() throws Exception
    {
        // given
        Log log = mock( Log.class );
        RobustJobSchedulerWrapper robustWrapper = new RobustJobSchedulerWrapper( actualScheduler, log );

        AtomicInteger count = new AtomicInteger();
        IllegalStateException e = new IllegalStateException();

        // when
        JobHandle jobHandle = robustWrapper.schedule( Group.HZ_TOPOLOGY_HEALTH, 100, () ->
        {
            count.incrementAndGet();
            throw e;
        } );

        // then
        assertEventually( "run count", count::get, Matchers.equalTo( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        jobHandle.waitTermination();
        verify( log, timeout( DEFAULT_TIMEOUT_MS ).times( 1 ) ).warn( "Uncaught exception", e );
    }

    @Test
    void recurringJobWithExceptionShouldKeepRunning() throws Exception
    {
        // given
        RobustJobSchedulerWrapper robustWrapper = new RobustJobSchedulerWrapper( actualScheduler, log );

        AtomicInteger count = new AtomicInteger();
        IllegalStateException e = new IllegalStateException();

        // when
        int nRuns = 100;
        JobHandle jobHandle = robustWrapper.scheduleRecurring( Group.HZ_TOPOLOGY_REFRESH, 1, () ->
        {
            if ( count.get() < nRuns )
            {
                count.incrementAndGet();
                throw e;
            }
        } );

        // then
        assertEventually( "run count", count::get, Matchers.equalTo( nRuns ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        jobHandle.cancel( true );
        verify( log, timeout( DEFAULT_TIMEOUT_MS ).times( nRuns ) ).warn( "Uncaught exception", e );
    }

    @Test
    void recurringJobWithErrorShouldStop() throws Exception
    {
        // given
        RobustJobSchedulerWrapper robustWrapper = new RobustJobSchedulerWrapper( actualScheduler, log );

        AtomicInteger count = new AtomicInteger();
        Error e = new Error();

        // when
        JobHandle jobHandle = robustWrapper.scheduleRecurring( Group.HZ_TOPOLOGY_REFRESH, 1, () ->
        {
            count.incrementAndGet();
            throw e;
        } );

        // when
        Thread.sleep( 50 ); // should not keep increasing during this time

        // then
        assertEventually( "run count", count::get, Matchers.equalTo( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        jobHandle.cancel( true );
        verify( log, timeout( DEFAULT_TIMEOUT_MS ).times( 1 ) ).error( "Uncaught error rethrown", e );
    }
}
