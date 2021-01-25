/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.database;

import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.metrics.global.MetricsManager;

import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.stats.CheckpointCounters;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.transaction.stats.TransactionLogCounters;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.memory.MemoryPools;
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

        RaftMonitors raftMonitors();

        Config configuration();

        MetricsManager metricsManager();

        JobScheduler scheduler();

        Database database();

        Supplier<TransactionIdStore> transactionIdStoreSupplier();

        TransactionCounters transactionCounters();

        Supplier<StoreEntityCounters> storeEntityCounters();

        Supplier<CoreMetaData> coreMetadataSupplier();

        TransactionLogCounters transactionLogCounters();

        CheckpointCounters checkpointCounters();

        FileSystemAbstraction fileSystem();

        Tracers tracers();

        MemoryPools memoryPools();
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
