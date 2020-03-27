/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import com.neo4j.metrics.source.db.BoltMetrics;
import com.neo4j.metrics.source.db.PageCacheMetrics;
import com.neo4j.metrics.source.jvm.FileDescriptorMetrics;
import com.neo4j.metrics.source.jvm.GCMetrics;
import com.neo4j.metrics.source.jvm.HeapMetrics;
import com.neo4j.metrics.source.jvm.MemoryBuffersMetrics;
import com.neo4j.metrics.source.jvm.MemoryPoolMetrics;
import com.neo4j.metrics.source.jvm.ThreadMetrics;
import com.neo4j.metrics.source.db.DatabaseOperationCountMetrics;
import com.neo4j.metrics.source.server.ServerMetrics;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class GlobalMetricsExporter
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final Config config;
    private final ExtensionContext context;
    private final Dependencies dependencies;

    GlobalMetricsExporter( MetricRegistry registry, Config config,
            ExtensionContext context, Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.config = config;
        this.context = context;
        this.dependencies = dependencies;
        this.life = life;
    }

    public void export()
    {
        String globalMetricsPrefix = config.get( MetricsSettings.metricsPrefix );
        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            life.add( new PageCacheMetrics( globalMetricsPrefix, registry, dependencies.pageCacheCounters() ) );
        }

        if ( config.get( MetricsSettings.jvmGcEnabled ) )
        {
            life.add( new GCMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.jvmHeapEnabled ) )
        {
            life.add( new HeapMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.jvmThreadsEnabled ) )
        {
            life.add( new ThreadMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.jvmMemoryEnabled ) )
        {
            life.add( new MemoryPoolMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.jvmBuffersEnabled ) )
        {
            life.add( new MemoryBuffersMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.jvmFileDescriptorsEnabled ) )
        {
            life.add( new FileDescriptorMetrics( globalMetricsPrefix, registry ) );
        }

        if ( config.get( MetricsSettings.boltMessagesEnabled ) )
        {
            life.add( new BoltMetrics( globalMetricsPrefix, registry, dependencies.monitors() ) );
        }

        if ( config.get( MetricsSettings.databaseOperationCountEnabled ) )
        {
            life.add( new DatabaseOperationCountMetrics( globalMetricsPrefix, registry, dependencies.monitors() ) );
        }

        boolean httpOrHttpsEnabled = config.get( HttpConnector.enabled ) || config.get( HttpsConnector.enabled );
        if ( httpOrHttpsEnabled && config.get( MetricsSettings.neoServerEnabled ) )
        {
            life.add( new ServerMetrics( globalMetricsPrefix, registry, dependencies.webContainerThreadInfo() ) );
        }
    }
}
