/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.tracers;

import java.time.Clock;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.TracerFactory;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

public class TimerTracerFactory implements TracerFactory
{
    private TimerTransactionTracer timerTransactionTracer = new TimerTransactionTracer();

    @Override
    public String getImplementationName()
    {
        return "timer";
    }

    @Override
    public PageCacheTracer createPageCacheTracer( Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock,
            Log log )
    {
        return PageCacheTracer.NULL;
    }

    @Override
    public TransactionTracer createTransactionTracer( Clock clock )
    {
        return timerTransactionTracer;
    }

    @Override
    public CheckPointTracer createCheckPointTracer( Clock clock )
    {
        return timerTransactionTracer;
    }
}
