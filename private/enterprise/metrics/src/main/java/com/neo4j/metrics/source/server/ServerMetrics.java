/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.server;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.web.WebContainerThreadInfo;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Server metrics" )
public class ServerMetrics extends LifecycleAdapter
{
    private static final String SERVER_PREFIX = "server";

    @Documented( "The total number of idle threads in the jetty pool. (gauge)" )
    private static final String THREAD_JETTY_IDLE_TEMPLATE = name( SERVER_PREFIX, "threads.jetty.idle" );
    @Documented( "The total number of threads (both idle and busy) in the jetty pool. (gauge)" )
    private static final String THREAD_JETTY_ALL_TEMPLATE = name( SERVER_PREFIX, "threads.jetty.all" );

    private final String threadJettyIdle;
    private final String threadJettyAll;

    private final MetricsRegister registry;
    private final Supplier<WebContainerThreadInfo> webContainerThreadInfo;

    public ServerMetrics( String metricsPrefix, MetricsRegister registry, Supplier<WebContainerThreadInfo> webThreadInfo )
    {
        this.registry = registry;
        this.threadJettyIdle = name( metricsPrefix, THREAD_JETTY_IDLE_TEMPLATE );
        this.threadJettyAll = name( metricsPrefix, THREAD_JETTY_ALL_TEMPLATE );
        this.webContainerThreadInfo = webThreadInfo;
    }

    @Override
    public void start()
    {
        registry.register( threadJettyIdle, () -> (Gauge<Integer>) () -> webContainerThreadInfo.get().idleThreads() );
        registry.register( threadJettyAll, () -> (Gauge<Integer>) () -> webContainerThreadInfo.get().allThreads() );
    }

    @Override
    public void stop()
    {
        registry.remove( threadJettyIdle );
        registry.remove( threadJettyAll );
    }
}
