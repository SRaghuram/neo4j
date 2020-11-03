/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.tracers;

import java.time.Clock;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.monitoring.tracing.TracerFactory;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

@ServiceProvider
public class TimerTracerFactory implements TracerFactory
{
    private TimerTransactionTracer timerTransactionTracer = new TimerTransactionTracer();

    @Override
    public String getName()
    {
        return "timer";
    }

    @Override
    public PageCacheTracer createPageCacheTracer( Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock, Log log, Config config )
    {
        return PageCacheTracer.NULL;
    }

    @Override
    public DatabaseTracer createDatabaseTracer( Clock clock )
    {
        return timerTransactionTracer;
    }
}
