/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.FakeClockJobScheduler;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ThroughputMonitorServiceTest
{
    private final FakeClockJobScheduler jobScheduler = new FakeClockJobScheduler();
    private final Duration sampleWindow = ofSeconds( 5 );

    private AssertableLogProvider logProvider = new AssertableLogProvider();

    private final ThroughputMonitorService throughputMonitorService =
            new ThroughputMonitorService( jobScheduler, jobScheduler, sampleWindow, logProvider );

    @Test
    void justCreatingTheMonitorShouldNotStartSampling() throws Exception
    {
        throughputMonitorService.start();

        var cit = mock( CommandIndexTracker.class );
        when( cit.getAppliedCommandIndex() ).thenReturn( 1L );
        var monitor = throughputMonitorService.createMonitor( cit );

        forward( sampleWindow.toSeconds() );
        LogAssertions.assertThat( logProvider ).doesNotHaveAnyLogs();
        verify( cit, never() ).getAppliedCommandIndex();

        monitor.start();
        forward( sampleWindow.toSeconds() );
        verify( cit, atLeast( 1 ) ).getAppliedCommandIndex();
    }

    @Test
    void shouldBeAbleToUnRegisterMonitors() throws Exception
    {
        ArrayList<ThroughputMonitor> monitors = registerMonitors();
        var removed = monitors.remove( 0 );
        throughputMonitorService.unregisterMonitor( removed );

        throughputMonitorService.start();
        forward( sampleWindow.toSeconds() );

        assertThat( removed.throughput() ).isEmpty();
        for ( ThroughputMonitor monitor : monitors )
        {
            assertEquals( 0.0, monitor.throughput().get() );
        }
    }

    @Test
    void shouldTriggerAllRegisteredMonitors() throws Exception
    {
        ArrayList<ThroughputMonitor> monitors = registerMonitors();

        throughputMonitorService.start();
        forward( sampleWindow.toSeconds() );
        for ( ThroughputMonitor monitor : monitors )
        {
            assertEquals( 0.0, monitor.throughput().get() );
        }
    }

    @Test
    void shouldStopSampling() throws Exception
    {
        ArrayList<ThroughputMonitor> monitors = registerMonitors();

        throughputMonitorService.start();
        throughputMonitorService.stop();
        forward( sampleWindow.toSeconds() );

        for ( ThroughputMonitor monitor : monitors )
        {
            assertThat( monitor.throughput() ).isEmpty();
        }
    }

    private ArrayList<ThroughputMonitor> registerMonitors()
    {
        var monitors = new ArrayList<ThroughputMonitor>();
        for ( int i = 0; i < 5; i++ )
        {
            var cit = new CommandIndexTracker();
            cit.setAppliedCommandIndex( 1 );
            var monitor = throughputMonitorService.createMonitor( cit );
            monitor.start();
            monitors.add( monitor );
        }
        return monitors;
    }

    private void forward( long seconds )
    {
        for ( int i = 0; i < seconds; i++ )
        {
            jobScheduler.forward( 1, TimeUnit.SECONDS );
        }
    }
}
