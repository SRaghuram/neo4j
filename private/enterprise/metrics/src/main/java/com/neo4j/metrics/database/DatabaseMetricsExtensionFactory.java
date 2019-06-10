/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.database;

import com.neo4j.causalclustering.common.ClusterMonitors;
import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.metrics.global.MetricsManager;

import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.kernel.extension.ExtensionType.DATABASE;

@ServiceProvider
public class DatabaseMetricsExtensionFactory extends ExtensionFactory<DatabaseMetricsExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Monitors monitors();

        ClusterMonitors clusterMonitors();

        Config configuration();

        MetricsManager metricsManager();

        JobScheduler scheduler();

        Database database();

        CheckPointerMonitor checkPointerMonitor();

        Supplier<TransactionIdStore> transactionIdStoreSupplier();

        TransactionCounters transactionCounters();

        LogRotationMonitor logRotationMonitor();

        StoreEntityCounters storeEntityCounters();

        Supplier<CoreMetaData> coreMetadataSupplier();
    }

    public DatabaseMetricsExtensionFactory()
    {
        super( DATABASE, "databaseMetrics" );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        return new DatabaseMetricsExtension( context, dependencies );
    }
}
