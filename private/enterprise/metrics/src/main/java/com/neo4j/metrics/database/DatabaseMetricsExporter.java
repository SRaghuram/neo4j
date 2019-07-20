/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.database;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.metrics.output.EventReporter;
import com.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import com.neo4j.metrics.source.causalclustering.CoreMetrics;
import com.neo4j.metrics.source.causalclustering.ReadReplicaMetrics;
import com.neo4j.metrics.source.db.CheckPointingMetrics;
import com.neo4j.metrics.source.db.CypherMetrics;
import com.neo4j.metrics.source.db.EntityCountMetrics;
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
    private final EventReporter reporter;
    private final Config config;
    private final ExtensionContext context;
    private final DatabaseMetricsExtensionFactory.Dependencies dependencies;

    DatabaseMetricsExporter( MetricRegistry registry, EventReporter reporter, Config config, ExtensionContext context,
            DatabaseMetricsExtensionFactory.Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.reporter = reporter;
        this.config = config;
        this.context = context;
        this.dependencies = dependencies;
        this.life = life;
    }

    public void export()
    {
        String metricsPrefix = databaseMetricsPrefix();

        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            life.add( new TransactionMetrics( metricsPrefix, registry, dependencies.transactionIdStoreSupplier(), dependencies.transactionCounters() ) );
        }

        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            life.add( new CheckPointingMetrics( metricsPrefix, reporter, registry, dependencies.monitors(), dependencies.checkPointerMonitor(),
                    dependencies.scheduler() ) );
        }

        if ( config.get( MetricsSettings.neoTransactionLogsEnabled ) )
        {
            life.add( new TransactionLogsMetrics( metricsPrefix, reporter, registry, dependencies.monitors(), dependencies.logRotationMonitor(),
                    dependencies.logAppenderMonitor(), dependencies.scheduler() ) );
        }

        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            if ( context.databaseInfo().edition != Edition.COMMUNITY && context.databaseInfo().edition != Edition.UNKNOWN )
            {
                life.add( new EntityCountMetrics( metricsPrefix, registry, dependencies.storeEntityCounters() ) );
            }
        }

        if ( config.get( MetricsSettings.cypherPlanningEnabled ) )
        {
            life.add( new CypherMetrics( metricsPrefix, registry, dependencies.monitors() ) );
        }

        if ( config.get( MetricsSettings.causalClusteringEnabled ) )
        {
            OperationalMode mode = context.databaseInfo().operationalMode;
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
        return config.get( MetricsSettings.metricsPrefix ) + "." + dependencies.database().getDatabaseId().name();
    }
}
