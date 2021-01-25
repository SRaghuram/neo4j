/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

@ServiceProvider
public class QueryLoggerExtension extends ExtensionFactory<QueryLoggerExtension.Dependencies>
{
    public interface Dependencies
    {
        FileSystemAbstraction fileSystem();

        Config config();

        Monitors monitoring();
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

        return new LifecycleAdapter()
        {
            DynamicLoggingQueryExecutionMonitor logger;

            @Override
            public void init()
            {
                this.logger = new DynamicLoggingQueryExecutionMonitor( config, fileSystem );
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
