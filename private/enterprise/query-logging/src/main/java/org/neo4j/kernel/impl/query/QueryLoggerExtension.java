/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.query;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

@ServiceProvider
public class QueryLoggerExtension extends ExtensionFactory<QueryLoggerExtension.Dependencies>
{
    public interface Dependencies
    {
        FileSystemAbstraction fileSystem();

        Config config();

        Monitors monitoring();

        LogService logger();

        JobScheduler jobScheduler();
    }

    public QueryLoggerExtension()
    {
        super( ExtensionType.GLOBAL, "query-logging" );
    }

    @Override
    public Lifecycle newInstance( @SuppressWarnings( "unused" ) ExtensionContext context, final Dependencies dependencies )
    {
        FileSystemAbstraction fileSystem = dependencies.fileSystem();
        Config config = dependencies.config();
        Monitors monitoring = dependencies.monitoring();
        LogService logService = dependencies.logger();
        JobScheduler jobScheduler = dependencies.jobScheduler();

        return new LifecycleAdapter()
        {
            DynamicLoggingQueryExecutionMonitor logger;

            @Override
            public void init()
            {
                Log debugLog = logService.getInternalLog( DynamicLoggingQueryExecutionMonitor.class );
                this.logger = new DynamicLoggingQueryExecutionMonitor( config, fileSystem, jobScheduler, debugLog );
                this.logger.init();
                monitoring.addMonitorListener( this.logger );
            }

            @Override
            public void shutdown()
            {
                logger.shutdown();
            }
        };
    }
}
