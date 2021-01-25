/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.monitoring.tracing.DefaultTracerFactory;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

@ServiceProvider
public class VerboseTracerFactory extends DefaultTracerFactory
{
    @Override
    public String getName()
    {
        return "verbose";
    }

    @Override
    public PageCacheTracer createPageCacheTracer( Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock, Log msgLog, Config config )
    {
        return new VerbosePageCacheTracer( msgLog, clock, config );
    }
}
