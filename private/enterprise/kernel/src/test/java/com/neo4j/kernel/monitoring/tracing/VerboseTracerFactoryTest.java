/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.monitoring.tracing;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.LogAssertions.assertThat;

class VerboseTracerFactoryTest
{
    @Test
    void verboseTracerFactoryRegisterTracerWithCodeNameVerbose()
    {
        assertEquals( VerboseTracerFactory.class, tracerFactory().getClass() );
    }

    @Test
    void verboseFactoryCreateVerboseTracer()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        PageCacheTracer pageCacheTracer = tracerFactory().createPageCacheTracer( new Monitors(),
                new OnDemandJobScheduler(), Clocks.nanoClock(), logProvider.getLog( "test" ), Config.defaults()  );
        pageCacheTracer.beginCacheFlush();
        assertThat( logProvider ).containsMessages( "Start whole page cache flush." );
    }

    private VerboseTracerFactory tracerFactory()
    {
        return new VerboseTracerFactory();
    }
}
