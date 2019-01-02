/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.global.GlobalMetricsExtensionFactory.Dependencies;
import org.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import org.neo4j.metrics.source.causalclustering.CoreMetrics;
import org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics;
import org.neo4j.metrics.source.db.BoltMetrics;
import org.neo4j.metrics.source.db.PageCacheMetrics;
import org.neo4j.metrics.source.jvm.GCMetrics;
import org.neo4j.metrics.source.jvm.MemoryBuffersMetrics;
import org.neo4j.metrics.source.jvm.MemoryPoolMetrics;
import org.neo4j.metrics.source.jvm.ThreadMetrics;
import org.neo4j.metrics.source.server.ServerMetrics;

public class GlobalMetricsExporter
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final Config config;
    private final LogService logService;
    private final KernelContext context;
    private final Dependencies dependencies;

    GlobalMetricsExporter( MetricRegistry registry, Config config, LogService logService,
            KernelContext context, Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.config = config;
        this.logService = logService;
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

        if ( config.get( MetricsSettings.boltMessagesEnabled ) )
        {
            life.add( new BoltMetrics( globalMetricsPrefix, registry, dependencies.monitors() ) );
        }

        if ( config.get( MetricsSettings.causalClusteringEnabled ) )
        {
            OperationalMode mode = context.databaseInfo().operationalMode;
            if ( mode == OperationalMode.CORE )
            {
                life.add( new CoreMetrics( globalMetricsPrefix, dependencies.monitors(), registry, dependencies.coreMetadataSupplier() ) );
                life.add( new CatchUpMetrics( globalMetricsPrefix, dependencies.monitors(), registry ) );
            }
            else if ( mode == OperationalMode.READ_REPLICA )
            {
                life.add( new ReadReplicaMetrics( globalMetricsPrefix, dependencies.monitors(), registry ) );
                life.add( new CatchUpMetrics( globalMetricsPrefix, dependencies.monitors(), registry ) );
            }
        }

        boolean httpOrHttpsEnabled = !config.enabledHttpConnectors().isEmpty();
        if ( httpOrHttpsEnabled && config.get( MetricsSettings.neoServerEnabled ) )
        {
            life.add( new ServerMetrics( globalMetricsPrefix, registry, logService, context.dependencySatisfier() ) );
        }
    }
}
