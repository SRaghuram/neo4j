/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source;

import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.LogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import org.neo4j.metrics.source.causalclustering.CoreMetrics;
import org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics;
import org.neo4j.metrics.source.db.BoltMetrics;
import org.neo4j.metrics.source.db.CheckPointingMetrics;
import org.neo4j.metrics.source.db.CypherMetrics;
import org.neo4j.metrics.source.db.EntityCountMetrics;
import org.neo4j.metrics.source.db.LogRotationMetrics;
import org.neo4j.metrics.source.db.PageCacheMetrics;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.metrics.source.jvm.GCMetrics;
import org.neo4j.metrics.source.jvm.MemoryBuffersMetrics;
import org.neo4j.metrics.source.jvm.MemoryPoolMetrics;
import org.neo4j.metrics.source.jvm.ThreadMetrics;
import org.neo4j.metrics.source.server.ServerMetrics;

public class Neo4jMetricsBuilder
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final EventReporter reporter;
    private final Config config;
    private final LogService logService;
    private final KernelContext kernelContext;
    private final Dependencies dependencies;

    public interface Dependencies
    {
        Monitors monitors();

        TransactionCounters transactionCounters();

        PageCacheCounters pageCacheCounters();

        Supplier<CoreMetaData> raft();

        DataSourceManager dataSourceManager();
    }

    public Neo4jMetricsBuilder( MetricRegistry registry, EventReporter reporter, Config config, LogService logService,
            KernelContext kernelContext, Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.reporter = reporter;
        this.config = config;
        this.logService = logService;
        this.kernelContext = kernelContext;
        this.dependencies = dependencies;
        this.life = life;
    }

    public boolean build()
    {
        boolean result = false;
        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            life.add( new TransactionMetrics( registry, databaseDependencySupplier( TransactionIdStore.class ),
                    dependencies.transactionCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            life.add( new PageCacheMetrics( registry, dependencies.pageCacheCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            life.add( new CheckPointingMetrics( reporter, registry, dependencies.monitors(),
                    databaseDependencySupplier( CheckPointerMonitor.class ) ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            life.add( new LogRotationMetrics( reporter, registry, dependencies.monitors(),
                    databaseDependencySupplier( LogRotationMonitor.class ) ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            if ( kernelContext.databaseInfo().edition != Edition.community &&
                    kernelContext.databaseInfo().edition != Edition.unknown )
            {
                life.add( new EntityCountMetrics( registry, databaseDependencySupplier( StoreEntityCounters.class ) ) );
                result = true;
            }
        }

        if ( config.get( MetricsSettings.cypherPlanningEnabled ) )
        {
            life.add( new CypherMetrics( registry, dependencies.monitors() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmGcEnabled ) )
        {
            life.add( new GCMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmThreadsEnabled ) )
        {
            life.add( new ThreadMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.boltMessagesEnabled ) )
        {
            life.add( new BoltMetrics( registry, dependencies.monitors() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmMemoryEnabled ) )
        {
            life.add( new MemoryPoolMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmBuffersEnabled ) )
        {
            life.add( new MemoryBuffersMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.causalClusteringEnabled ) )
        {
            OperationalMode mode = kernelContext.databaseInfo().operationalMode;
            if ( mode == OperationalMode.core )
            {
                life.add( new CoreMetrics( dependencies.monitors(), registry, dependencies.raft() ) );
                life.add( new CatchUpMetrics( dependencies.monitors(), registry ) );
                result = true;
            }
            else if ( mode == OperationalMode.read_replica )
            {
                life.add( new ReadReplicaMetrics( dependencies.monitors(), registry ) );
                life.add( new CatchUpMetrics( dependencies.monitors(), registry ) );
                result = true;
            }
        }

        boolean httpOrHttpsEnabled = !config.enabledHttpConnectors().isEmpty();
        if ( httpOrHttpsEnabled && config.get( MetricsSettings.neoServerEnabled ) )
        {
            life.add( new ServerMetrics( registry, logService, kernelContext.dependencySatisfier() ) );
            result = true;
        }

        return result;
    }

    private <T> Supplier<T> databaseDependencySupplier( Class<T> clazz )
    {
        return () -> dependencies.dataSourceManager().getDataSource().getDependencyResolver().resolveDependency( clazz );
    }
}
