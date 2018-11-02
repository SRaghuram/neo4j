/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

/**
 * Default implementation of {@link ConversationSPI} used on master in HA setup.
 */
public class DefaultConversationSPI implements ConversationSPI
{
    private final Locks locks;
    private final JobScheduler jobScheduler;

    public DefaultConversationSPI( Locks locks, JobScheduler jobScheduler )
    {
        this.locks = locks;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public Locks.Client acquireClient()
    {
        return locks.newClient();
    }

    @Override
    public JobHandle scheduleRecurringJob( Group group, long interval, Runnable job )
    {
        return jobScheduler.scheduleRecurring( group, job, interval, TimeUnit.MILLISECONDS);
    }
}
