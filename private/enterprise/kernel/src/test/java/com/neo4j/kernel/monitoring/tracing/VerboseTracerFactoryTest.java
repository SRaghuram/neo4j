/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import org.junit.Test;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.BufferingLog;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;

public class VerboseTracerFactoryTest
{

    @Test
    public void verboseTracerFactoryRegisterTracerWithCodeNameVerbose()
    {
        assertEquals( "verbose", tracerFactory().getImplementationName() );
    }

    @Test
    public void verboseFactoryCreateVerboseTracer()
    {
        BufferingLog msgLog = new BufferingLog();
        PageCacheTracer pageCacheTracer = tracerFactory().createPageCacheTracer( new Monitors(),
                new OnDemandJobScheduler(), Clocks.nanoClock(), msgLog );
        pageCacheTracer.beginCacheFlush();
        assertEquals( "Start whole page cache flush.", msgLog.toString().trim() );
    }

    private VerboseTracerFactory tracerFactory()
    {
        return new VerboseTracerFactory();
    }
}
