/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;
import org.neo4j.metrics.source.Neo4jMetricsBuilder;
import org.neo4j.scheduler.JobScheduler;

public class MetricsKernelExtensionFactory extends KernelExtensionFactory<MetricsKernelExtensionFactory.Dependencies>
{
    public interface Dependencies extends Neo4jMetricsBuilder.Dependencies
    {
        Config configuration();

        LogService logService();

        FileSystemAbstraction fileSystemAbstraction();

        JobScheduler scheduler();

        ConnectorPortRegister portRegister();
    }

    public MetricsKernelExtensionFactory()
    {
        super( "metrics" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        return new MetricsExtension( context, dependencies );
    }
}
