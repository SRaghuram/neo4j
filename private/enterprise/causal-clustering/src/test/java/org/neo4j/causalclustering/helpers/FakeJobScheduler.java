/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helpers;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobSchedulerAdapter;

import static org.mockito.Mockito.mock;

public class FakeJobScheduler extends JobSchedulerAdapter
{
    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        job.run();
        return mock( JobHandle.class );
    }
}

