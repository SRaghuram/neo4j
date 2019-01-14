/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class DefaultConversationSPITest
{

    @Mock( answer = Answers.RETURNS_MOCKS )
    private Locks locks;
    @Mock
    private JobScheduler jobScheduler;
    @InjectMocks
    private DefaultConversationSPI conversationSpi;

    @Test
    public void testAcquireClient()
    {
        conversationSpi.acquireClient();

        verify(locks).newClient();
    }

    @Test
    public void testScheduleRecurringJob()
    {
        Runnable job = mock( Runnable.class );
        conversationSpi.scheduleRecurringJob( Group.SLAVE_LOCKS_TIMEOUT, 0, job );

        verify( jobScheduler ).scheduleRecurring( Group.SLAVE_LOCKS_TIMEOUT, job, 0, TimeUnit.MILLISECONDS );
    }
}
