/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.neo4j.configuration.MetricsSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.web.WebContainerThreadInfo;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Neo4jLayoutExtension
class GlobalMetricsExtensionTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private Neo4jLayout neo4jLayout;
    private ExtensionContext context;

    @BeforeEach
    void setUp()
    {
        context = new GlobalExtensionContext( neo4jLayout, DbmsInfo.TOOL, new Dependencies() );
    }

    @Test
    void extensionCanBeStartedWithoutRegisteredReporters()
    {
        Config config = Config.defaults( MetricsSettings.csv_enabled, false );
        GlobalMetricsDependencies metricsDependencies = new GlobalMetricsDependencies( config );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension ) )
            {
                assertFalse( globalMetricsExtension.isConfigured() );
            }
        } );
    }

    @Test
    void extensionCanBeStartedWhenMetricsDisabled()
    {
        Config config = Config.newBuilder()
                .set( MetricsSettings.metrics_enabled, false )
                .build();
        GlobalMetricsDependencies metricsDependencies = new GlobalMetricsDependencies( config );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension ) )
            {
                assertFalse( globalMetricsExtension.isConfigured() );
            }
        } );
    }

    @Test
    void globalExtensionProvideMetricsRegistryAndReporter()
    {
        Config config = Config.defaults( MetricsSettings.metrics_enabled, false );
        GlobalMetricsDependencies metricsDependencies = new GlobalMetricsDependencies( config );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension ) )
            {
                assertFalse( globalMetricsExtension.isConfigured() );
                assertNotNull( globalMetricsExtension.getRegistry() );
                assertNotNull( globalMetricsExtension.getReporter() );
            }
        } );
    }

    private class GlobalMetricsDependencies implements GlobalMetricsExtensionFactory.Dependencies
    {
        private final Config config;

        GlobalMetricsDependencies( Config config )
        {
            this.config = config;
        }

        @Override
        public Monitors monitors()
        {
            return null;
        }

        @Override
        public PageCacheCounters pageCacheCounters()
        {
            return null;
        }

        @Override
        public DatabaseOperationCounts databaseOperationCounts()
        {
            return null;
        }

        @Override
        public Config configuration()
        {
            return config;
        }

        @Override
        public LogService logService()
        {
            return new SimpleLogService( NullLogProvider.getInstance() );
        }

        @Override
        public FileSystemAbstraction fileSystemAbstraction()
        {
            return fileSystem;
        }

        @Override
        public JobScheduler scheduler()
        {
            return null;
        }

        @Override
        public ConnectorPortRegister portRegister()
        {
            return null;
        }

        @Override
        public Supplier<WebContainerThreadInfo> webContainerThreadInfo()
        {
            return null;
        }

        @Override
        public MemoryPools memoryPools()
        {
            return null;
        }
    }
}
