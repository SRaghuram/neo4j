/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.causalclustering.DiscoveryCoreMetrics;
import com.neo4j.metrics.source.db.BoltMetrics;
import com.neo4j.metrics.source.db.DatabaseOperationCountMetrics;
import com.neo4j.metrics.source.db.GlobalMemoryPoolMetrics;
import com.neo4j.metrics.source.db.PageCacheMetrics;
import com.neo4j.metrics.source.jvm.FileDescriptorMetrics;
import com.neo4j.metrics.source.jvm.GCMetrics;
import com.neo4j.metrics.source.jvm.HeapMetrics;
import com.neo4j.metrics.source.jvm.JVMMemoryBuffersMetrics;
import com.neo4j.metrics.source.jvm.JVMMemoryPoolMetrics;
import com.neo4j.metrics.source.jvm.PauseMetrics;
import com.neo4j.metrics.source.jvm.ThreadMetrics;
import com.neo4j.metrics.source.server.ServerMetrics;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class GlobalMetricsExporter
{
    private final MetricsRegister registry;
    private final LifeSupport life;
    private final Config config;
    private final ExtensionContext context;
    private final Dependencies dependencies;

    GlobalMetricsExporter( MetricsRegister registry, Config config,
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
        String globalMetricsPrefix = config.get( MetricsSettings.metrics_namespaces_enabled ) ?
                                     config.get( MetricsSettings.metrics_prefix ) + ".dbms" : config.get( MetricsSettings.metrics_prefix );

        life.add( new PageCacheMetrics( globalMetricsPrefix, registry, dependencies.pageCacheCounters() ) );

        life.add( new GCMetrics( globalMetricsPrefix, registry ) );

        life.add( new HeapMetrics( globalMetricsPrefix, registry ) );

        life.add( new ThreadMetrics( globalMetricsPrefix, registry ) );

        life.add( new JVMMemoryPoolMetrics( globalMetricsPrefix, registry ) );

        life.add( new JVMMemoryBuffersMetrics( globalMetricsPrefix, registry ) );

        life.add( new FileDescriptorMetrics( globalMetricsPrefix, registry ) );

        life.add( new PauseMetrics( globalMetricsPrefix, registry, dependencies.monitors() ) );

        if ( config.get( GraphDatabaseSettings.mode ) == GraphDatabaseSettings.Mode.CORE )
        {
            life.add( new DiscoveryCoreMetrics( globalMetricsPrefix, dependencies.monitors(), registry ) );
        }

        life.add( new BoltMetrics( globalMetricsPrefix, registry, dependencies.monitors() ) );

        life.add( new DatabaseOperationCountMetrics( globalMetricsPrefix, registry, dependencies.databaseOperationCounts() ) );

        // GlobalMemoryPoolMetrics already contains dbms in its name, therefore its prefix doesn't depend on metrics_namespaces_enabled.
        life.add( new GlobalMemoryPoolMetrics( config.get( MetricsSettings.metrics_prefix ), registry, dependencies.memoryPools() ) );

        boolean httpOrHttpsEnabled = config.get( HttpConnector.enabled ) || config.get( HttpsConnector.enabled );
        if ( httpOrHttpsEnabled )
        {
            life.add( new ServerMetrics( globalMetricsPrefix, registry, dependencies.webContainerThreadInfo() ) );
        }
    }
}
