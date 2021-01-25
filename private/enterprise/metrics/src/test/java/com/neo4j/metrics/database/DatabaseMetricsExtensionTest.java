/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.database;

import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.metrics.global.GlobalMetricsExtension;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory;
import com.neo4j.metrics.global.MetricsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.stats.CheckpointCounters;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.transaction.stats.TransactionLogCounters;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.web.WebContainerThreadInfo;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Suppliers.singleton;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

@Neo4jLayoutExtension
class DatabaseMetricsExtensionTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    private ExtensionContext context;

    private static final NamedDatabaseId DATABASE_ID = randomNamedDatabaseId();

    @BeforeEach
    void setUp()
    {
        context = new DatabaseExtensionContext( databaseLayout, DbmsInfo.TOOL, new Dependencies() );
    }

    @Test
    void extensionCanBeStartedWithoutRegisteredReporters()
    {
        Config config = Config.defaults( MetricsSettings.csv_enabled, false );
        DatabaseMetricsDependencies metricsDependencies = new DatabaseMetricsDependencies( config );
        DatabaseMetricsExtension databaseMetricsExtension = new DatabaseMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( databaseMetricsExtension ) )
            {
                // empty
            }
        } );
    }

    @Test
    void extensionCanBeStartedWhenMetricsDisabled()
    {
        Config config = Config.defaults( MetricsSettings.metrics_enabled, false );
        DatabaseMetricsDependencies metricsDependencies = new DatabaseMetricsDependencies( config );
        DatabaseMetricsExtension databaseMetricsExtension = new DatabaseMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( databaseMetricsExtension ) )
            {
                // empty
            }
        } );
    }

    @Test
    void registerDatabaseMetricsInGlobalMetricsRegistry()
    {
        Config config = Config.defaults();
        config.set( MetricsSettings.metrics_filter, GlobbingPattern.create( "*" ) );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, new GlobalMetricsDependencies( config ) );
        DatabaseMetricsDependencies metricsDependencies = new DatabaseMetricsDependencies( config, globalMetricsExtension );
        DatabaseMetricsExtension databaseMetricsExtension = new DatabaseMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension, databaseMetricsExtension ) )
            {
                assertThat( globalMetricsExtension.getRegistry().getNames() )
                        .contains( format( "neo4j.%s.check_point.events", DATABASE_ID.name() ) );
            }
        } );
    }

    @Test
    void namespacesEnabledShouldGroupGlobalAndDatabaseMetrics()
    {
        Config config = Config.defaults( MetricsSettings.metrics_namespaces_enabled, true );
        GlobalMetricsExtension globalMetricsExtension = new GlobalMetricsExtension( context, new GlobalMetricsDependencies( config ) );
        DatabaseMetricsDependencies metricsDependencies = new DatabaseMetricsDependencies( config, globalMetricsExtension );
        DatabaseMetricsExtension databaseMetricsExtension = new DatabaseMetricsExtension( context, metricsDependencies );

        assertDoesNotThrow( () ->
        {
            try ( Lifespan ignored = new Lifespan( globalMetricsExtension, databaseMetricsExtension ) )
            {
                globalMetricsExtension.getRegistry().getNames().forEach(
                        metric -> assertThat( metric ).satisfiesAnyOf(
                                ( String name ) -> assertThat( name ).startsWith( "neo4j.dbms" ),
                                ( String name ) -> assertThat( name ).startsWith( "neo4j.database." + DATABASE_ID.name() ) ) );
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
            return new Monitors();
        }

        @Override
        public PageCacheCounters pageCacheCounters()
        {
            return PageCacheTracer.NULL;
        }

        @Override
        public DatabaseOperationCounts databaseOperationCounts()
        {
            return new DatabaseOperationCounts.Counter();
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
            return new MemoryPools();
        }
    }

    private class DatabaseMetricsDependencies implements DatabaseMetricsExtensionFactory.Dependencies
    {
        private final Config config;
        private final MetricsManager metricsManager;

        DatabaseMetricsDependencies( Config config )
        {
            this( config, null );
        }

        DatabaseMetricsDependencies( Config config, MetricsManager metricsManager )
        {
            this.config = config;
            this.metricsManager = metricsManager;
        }

        @Override
        public Monitors monitors()
        {
            return new Monitors();
        }

        @Override
        public RaftMonitors raftMonitors()
        {
            return null;
        }

        @Override
        public Config configuration()
        {
            return config;
        }

        @Override
        public MetricsManager metricsManager()
        {
            if ( metricsManager != null )
            {
                return metricsManager;
            }
            throw new UnsatisfiedDependencyException( MetricsManager.class );
        }

        @Override
        public JobScheduler scheduler()
        {
            return new CallingThreadJobScheduler();
        }

        @Override
        public Database database()
        {
            Database database = mock( Database.class );
            when( database.getNamedDatabaseId() ).thenReturn( DATABASE_ID );
            when( database.getDatabaseLayout() ).thenReturn( databaseLayout );
            return database;
        }

        @Override
        public CheckpointCounters checkpointCounters()
        {
            return new DefaultTracer();
        }

        @Override
        public FileSystemAbstraction fileSystem()
        {
            return fileSystem;
        }

        @Override
        public Tracers tracers()
        {
            return null;
        }

        @Override
        public MemoryPools memoryPools()
        {
            return new MemoryPools();
        }

        @Override
        public Supplier<TransactionIdStore> transactionIdStoreSupplier()
        {
            return singleton( mock( TransactionIdStore.class ) );
        }

        @Override
        public TransactionCounters transactionCounters()
        {
            return new DatabaseTransactionStats();
        }

        @Override
        public Supplier<StoreEntityCounters> storeEntityCounters()
        {
            return null;
        }

        @Override
        public Supplier<CoreMetaData> coreMetadataSupplier()
        {
            return null;
        }

        @Override
        public TransactionLogCounters transactionLogCounters()
        {
            return new DefaultTracer();
        }
    }

}
