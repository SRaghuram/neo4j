/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.database;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import com.neo4j.metrics.source.causalclustering.CoreMetrics;
import com.neo4j.metrics.source.causalclustering.ReadReplicaMetrics;
import com.neo4j.metrics.source.db.CheckPointingMetrics;
import com.neo4j.metrics.source.db.CypherMetrics;
import com.neo4j.metrics.source.db.DatabaseCountMetrics;
import com.neo4j.metrics.source.db.DatabaseMemoryPoolMetrics;
import com.neo4j.metrics.source.db.EntityCountMetrics;
import com.neo4j.metrics.source.db.StoreSizeMetrics;
import com.neo4j.metrics.source.db.TransactionLogsMetrics;
import com.neo4j.metrics.source.db.TransactionMetrics;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class DatabaseMetricsExporter
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final Config config;
    private final ExtensionContext context;
    private final DatabaseMetricsExtensionFactory.Dependencies dependencies;

    DatabaseMetricsExporter( MetricRegistry registry, Config config, ExtensionContext context,
            DatabaseMetricsExtensionFactory.Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.config = config;
        this.context = context;
        this.dependencies = dependencies;
        this.life = life;
    }

    public void export()
    {
        String metricsPrefix = databaseMetricsPrefix();

        if ( config.get( MetricsSettings.neo_tx_enabled ) )
        {
            life.add( new TransactionMetrics( metricsPrefix, registry, dependencies.transactionIdStoreSupplier(), dependencies.transactionCounters() ) );
        }

        if ( config.get( MetricsSettings.neo_check_pointing_enabled ) )
        {
            life.add( new CheckPointingMetrics( metricsPrefix, registry, dependencies.checkpointCounters() ) );
        }

        if ( config.get( MetricsSettings.neo_transaction_logs_enabled ) )
        {
            life.add( new TransactionLogsMetrics( metricsPrefix, registry, dependencies.transactionLogCounters() ) );
        }

        if ( config.get( MetricsSettings.neo_counts_enabled ) )
        {
            if ( context.dbmsInfo().edition != Edition.COMMUNITY && context.dbmsInfo().edition != Edition.UNKNOWN )
            {
                life.add( new EntityCountMetrics( metricsPrefix, registry, dependencies.storeEntityCounters() ) );
            }
        }

        if ( config.get( MetricsSettings.database_counts_enabled ) )
        {
            if ( context.dbmsInfo().edition != Edition.COMMUNITY && context.dbmsInfo().edition != Edition.UNKNOWN )
            {
                var pageCacheTracer = dependencies.tracers().getPageCacheTracer();
                life.add( new DatabaseCountMetrics( metricsPrefix, registry, dependencies.storeEntityCounters(), pageCacheTracer ) );
            }
        }

        if ( config.get( MetricsSettings.neo_store_size_enabled ) )
        {
            life.add( new StoreSizeMetrics( metricsPrefix, registry, dependencies.scheduler(), dependencies.fileSystem(),
                    dependencies.database().getDatabaseLayout() ) );
        }

        if ( config.get( MetricsSettings.cypher_planning_enabled ) )
        {
            life.add( new CypherMetrics( metricsPrefix, registry, dependencies.monitors() ) );
        }

        if ( config.get( MetricsSettings.neo_memory_pools_enabled ) )
        {
            life.add( new DatabaseMemoryPoolMetrics( metricsPrefix, registry, dependencies.memoryPools(),
                            dependencies.database().getNamedDatabaseId().name() ) );
        }

        if ( config.get( MetricsSettings.causal_clustering_enabled ) )
        {
            OperationalMode mode = context.dbmsInfo().operationalMode;
            if ( mode == OperationalMode.CORE )
            {
                life.add( new CoreMetrics( metricsPrefix, dependencies.clusterMonitors(), registry, dependencies.coreMetadataSupplier() ) );
                life.add( new CatchUpMetrics( metricsPrefix, dependencies.monitors(), registry ) );
            }
            else if ( mode == OperationalMode.READ_REPLICA )
            {
                life.add( new ReadReplicaMetrics( metricsPrefix, dependencies.clusterMonitors(), registry ) );
                life.add( new CatchUpMetrics( metricsPrefix, dependencies.monitors(), registry ) );
            }
        }
    }

    private String databaseMetricsPrefix()
    {
        return config.get( MetricsSettings.metrics_prefix ) + "." + dependencies.database().getNamedDatabaseId().name();
    }
}
