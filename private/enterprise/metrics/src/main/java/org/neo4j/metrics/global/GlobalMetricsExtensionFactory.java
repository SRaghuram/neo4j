/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

public class GlobalMetricsExtensionFactory extends KernelExtensionFactory<GlobalMetricsExtensionFactory.Dependencies>
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
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        return new GlobalMetricsExtension( context, dependencies );
    }
}
