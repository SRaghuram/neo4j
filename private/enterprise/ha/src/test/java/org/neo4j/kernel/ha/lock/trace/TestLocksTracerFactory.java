/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock.trace;

import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.DefaultTracerFactory;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.lock.LockTracer;

public class TestLocksTracerFactory extends DefaultTracerFactory
{
    public TestLocksTracerFactory()
    {
    }

    @Override
    public String getImplementationName()
    {
        return "slaveLocksTracer";
    }

    @Override
    public LockTracer createLockTracer( Monitors monitors, JobScheduler jobScheduler )
    {
        return new RecordingLockTracer();
    }
}
