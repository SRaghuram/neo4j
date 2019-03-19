/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.neo4j.causalclustering.core.consensus.CoreMetaData;

import java.util.function.Supplier;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

@ServiceProvider
public class GlobalMetricsExtensionFactory extends ExtensionFactory<GlobalMetricsExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Monitors monitors();

        PageCacheCounters pageCacheCounters();

        Config configuration();

        LogService logService();

        FileSystemAbstraction fileSystemAbstraction();

        JobScheduler scheduler();

        Supplier<CoreMetaData> coreMetadataSupplier();

        ConnectorPortRegister portRegister();
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
