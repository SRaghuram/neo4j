/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;

class CatchupJobSchedulerTest
{
    private final StubCatchupJob stubCatchupJob = new StubCatchupJob();
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final Duration pullInterval = Duration.ofSeconds( 1 );
    private final OnDemandTimerService timerService = new OnDemandTimerService( fakeClock );
    private final CatchupJobScheduler catchupJobScheduler = new CatchupJobScheduler( timerService, stubCatchupJob, pullInterval );

    @Test
    void shouldExecuteEveryIntervalWhenStarted() throws Exception
    {
        // given
        catchupJobScheduler.start();

        // when
        forwardOneInterval();

        // then
        assertThat( stubCatchupJob.executeCount ).isEqualTo( 1 );

        // when
        forwardOneInterval();

        // then
        assertThat( stubCatchupJob.executeCount ).isEqualTo( 2 );
    }

    @Test
    void shouldStopExecutingWhenStopped() throws Exception
    {
        // given
        catchupJobScheduler.start();

        // when
        fakeClock.forward( pullInterval );
        forwardOneInterval();

        //when
        catchupJobScheduler.stop();
        forwardOneInterval();

        // then
        assertThat( stubCatchupJob.executeCount ).isEqualTo( 2 ); // still the same
    }

    @Test
    void shouldStopExecutingWhenUnableToSchedule() throws Exception
    {
        // given
        catchupJobScheduler.start();

        // when
        stubCatchupJob.canSchedule = false;

        // and
        forwardOneInterval();

        // then
        assertThat( stubCatchupJob.executeCount ).isZero();
    }

    private void forwardOneInterval()
    {
        fakeClock.forward( pullInterval );
        timerService.invoke( CatchupJobScheduler.Timers.TX_PULLER_TIMER );
    }

    private static class StubCatchupJob implements CatchupJob
    {
        int executeCount ;
        boolean canSchedule = true;

        @Override
        public void execute()
        {
            executeCount++;
        }

        @Override
        public boolean canSchedule()
        {
            return canSchedule;
        }
    }
}
