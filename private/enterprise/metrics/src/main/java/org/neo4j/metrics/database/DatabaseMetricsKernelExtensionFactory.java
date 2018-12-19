/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.database;

import java.util.function.Supplier;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.global.MetricsManager;

import static org.neo4j.kernel.extension.ExtensionType.DATABASE;

public class DatabaseMetricsKernelExtensionFactory extends KernelExtensionFactory<DatabaseMetricsKernelExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Monitors monitors();

        Config configuration();

        MetricsManager metricsManager();

        Database database();

        CheckPointerMonitor checkPointerMonitor();

        Supplier<TransactionIdStore> transactionIdStoreSupplier();

        TransactionCounters transactionCounters();

        LogRotationMonitor logRotationMonitor();

        StoreEntityCounters storeEntityCounters();
    }

    public DatabaseMetricsKernelExtensionFactory()
    {
        super( DATABASE, "databaseMetrics" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        return new DatabaseMetricsExtension( context, dependencies );
    }
}
