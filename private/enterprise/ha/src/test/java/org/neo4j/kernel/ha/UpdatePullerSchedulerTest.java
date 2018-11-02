/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class UpdatePullerSchedulerTest
{
    private UpdatePuller updatePuller;

    @Before
    public void setUp()
    {
        updatePuller = mock( UpdatePuller.class );
    }

    @Test
    public void skipUpdatePullingSchedulingWithZeroInterval()
    {
        JobScheduler jobScheduler = mock( JobScheduler.class );
        UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 0 );

        // when start puller scheduler - nothing should be scheduled
        pullerScheduler.init();

        verifyZeroInteractions( jobScheduler, updatePuller );

        // should be able shutdown scheduler
        pullerScheduler.shutdown();
    }

    @Test
    public void scheduleUpdatePulling() throws Throwable
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler( false );
        UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 10 );

        // schedule update pulling and run it
        pullerScheduler.init();
        jobScheduler.runJob();

        verify( updatePuller ).pullUpdates();
        assertNotNull( "Job should be scheduled", jobScheduler.getJob() );

        // stop scheduler - job should be canceled
        pullerScheduler.shutdown();

        assertNull( "Job should be canceled", jobScheduler.getJob() );
    }

}
