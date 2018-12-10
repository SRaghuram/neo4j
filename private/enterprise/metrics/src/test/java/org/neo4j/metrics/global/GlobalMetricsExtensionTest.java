/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Supplier;

import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class GlobalMetricsExtensionTest
{
    @Inject
    private TestDirectory testDirectory;
    private ExtensionContext context;

    @BeforeEach
    void setUp()
    {
        context = new GlobalExtensionContext( testDirectory.storeLayout(), DatabaseInfo.TOOL, new Dependencies() );
    }

    @Test
    void extensionCanBeStartedWithoutRegisteredReporters()
    {
        Config config = Config.defaults( MetricsSettings.csvEnabled, Settings.FALSE );
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
        Config config = Config.defaults( MetricsSettings.metricsEnabled, Settings.FALSE );
        GlobalMetricsDependencies metricsDependencies = new GlobalMetricsDependencies( config );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension ) )
            {
                assertTrue( globalMetricsExtension.isConfigured() );
            }
        } );
    }

    @Test
    void globalExtensionProvideMetricsRegistryAndReporter()
    {
        Config config = Config.defaults( MetricsSettings.metricsEnabled, Settings.FALSE );
        GlobalMetricsDependencies metricsDependencies = new GlobalMetricsDependencies( config );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension ) )
            {
                assertTrue( globalMetricsExtension.isConfigured() );
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
            return testDirectory.getFileSystem();
        }

        @Override
        public JobScheduler scheduler()
        {
            return null;
        }

        @Override
        public Supplier<CoreMetaData> coreMetadataSupplier()
        {
            return null;
        }

        @Override
        public ConnectorPortRegister portRegister()
        {
            return null;
        }
    }
}
