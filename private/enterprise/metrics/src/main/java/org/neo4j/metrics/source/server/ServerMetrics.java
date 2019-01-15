/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.server;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Server metrics" )
public class ServerMetrics extends LifecycleAdapter
{
    private static final String SERVER_PREFIX = "server";

    @Documented( "The total number of idle threads in the jetty pool." )
    private static final String THREAD_JETTY_IDLE_TEMPLATE = name( SERVER_PREFIX, "threads.jetty.idle" );
    @Documented( "The total number of threads (both idle and busy) in the jetty pool." )
    private static final String THREAD_JETTY_ALL_TEMPLATE = name( SERVER_PREFIX, "threads.jetty.all" );

    private final String threadJettyIdle;
    private final String threadJettyAll;

    private final MetricRegistry registry;
    private volatile ServerThreadView serverThreadView;

    public ServerMetrics( String metricsPrefix, MetricRegistry registry, LogService logService, DependencySatisfier satisfier )
    {
        Log userLog = logService.getUserLog( getClass() );
        this.registry = registry;
        this.threadJettyIdle = name( metricsPrefix, THREAD_JETTY_IDLE_TEMPLATE );
        this.threadJettyAll = name( metricsPrefix, THREAD_JETTY_ALL_TEMPLATE );
        this.serverThreadView = new ServerThreadView()
        {
            private volatile boolean warnedAboutIdle;
            private volatile boolean warnedAboutAll;
            @Override
            public int idleThreads()
            {
                if ( !warnedAboutIdle )
                {
                    userLog.warn( "Server thread metrics not available (missing " + threadJettyIdle + ")" );
                    warnedAboutIdle = true;
                }
                return -1;
            }

            @Override
            public int allThreads()
            {
                if ( !warnedAboutAll )
                {
                    userLog.warn( "Server thread metrics not available (missing " + threadJettyAll + ")" );
                    warnedAboutAll = true;
                }
                return -1;
            }
        };
        satisfier.satisfyDependency( (ServerThreadViewSetter) serverThreadView ->
        {
            assert ServerMetrics.this.serverThreadView != null;
            ServerMetrics.this.serverThreadView = serverThreadView;
            userLog.info( "Server thread metrics have been registered successfully" );
        } );
    }

    @Override
    public void start()
    {
        registry.register( threadJettyIdle, (Gauge<Integer>) () -> serverThreadView.idleThreads() );
        registry.register( threadJettyAll, (Gauge<Integer>) () -> serverThreadView.allThreads() );
    }

    @Override
    public void stop()
    {
        registry.remove( threadJettyIdle );
        registry.remove( threadJettyAll );
    }
}
