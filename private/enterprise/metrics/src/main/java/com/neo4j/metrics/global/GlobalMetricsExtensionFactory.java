/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.web.WebContainerThreadInfo;

@ServiceProvider
public class GlobalMetricsExtensionFactory extends ExtensionFactory<GlobalMetricsExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Monitors monitors();

        PageCacheCounters pageCacheCounters();

        DatabaseOperationCounts databaseOperationCounts();

        Config configuration();

        LogService logService();

        FileSystemAbstraction fileSystemAbstraction();

        JobScheduler scheduler();

        ConnectorPortRegister portRegister();

        Supplier<WebContainerThreadInfo> webContainerThreadInfo();

        MemoryPools memoryPools();
    }

    public GlobalMetricsExtensionFactory()
    {
        super( "globalMetrics" );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        return new GlobalMetricsExtension( context, dependencies );
    }
}
