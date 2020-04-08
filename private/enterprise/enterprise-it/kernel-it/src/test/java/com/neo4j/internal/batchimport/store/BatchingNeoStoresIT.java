/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport.store;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.metrics.global.GlobalMetricsExtension;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;

@Neo4jLayoutExtension
class BatchingNeoStoresIT
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    private AssertableLogProvider provider;
    private SimpleLogService logService;

    @BeforeEach
    void setUp()
    {
        provider = new AssertableLogProvider();
        logService = new SimpleLogService( provider, provider );
    }

    @Test
    void startBatchingNeoStoreWithMetricsPluginEnabled() throws Exception
    {
        Config config = Config.defaults( MetricsSettings.metricsEnabled, true );
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, databaseLayout, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, AdditionalInitialIds.EMPTY, config, jobScheduler, PageCacheTracer.NULL ) )
        {
            batchingNeoStores.createNew();
        }
        assertThat( provider ).forClass( GlobalMetricsExtension.class ).doesNotHaveAnyLogs();
    }

    @Test
    void createStoreWithNotEmptyInitialIds() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, databaseLayout, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, new TestAdditionalInitialIds(), Config.defaults(), jobScheduler, PageCacheTracer.NULL ) )
        {
            batchingNeoStores.createNew();
        }

        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder( databaseLayout )
                .setConfig( GraphDatabaseSettings.fail_on_missing_files, false )
                .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            TransactionIdStore transactionIdStore = getTransactionIdStore( (GraphDatabaseAPI) database );
            assertEquals( 10, transactionIdStore.getLastCommittedTransactionId() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void tracePageCacheAccessOnEmptyStoreCreation() throws Exception
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores batchingNeoStores = BatchingNeoStores
                        .batchingNeoStores( fileSystem, databaseLayout, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                                logService, new TestAdditionalInitialIds(), Config.defaults(), jobScheduler, pageCacheTracer ) )
        {
            batchingNeoStores.createNew();
        }

        assertThat( pageCacheTracer.pins() ).isEqualTo( 387 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 387 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 368 );
        assertThat( pageCacheTracer.faults() ).isEqualTo( 19 );
    }

    private static TransactionIdStore getTransactionIdStore( GraphDatabaseAPI database )
    {
        DependencyResolver resolver = database.getDependencyResolver();
        return resolver.resolveDependency( TransactionIdStore.class );
    }

    private static class TestAdditionalInitialIds implements AdditionalInitialIds
    {
        @Override
        public long lastCommittedTransactionId()
        {
            return 10;
        }

        @Override
        public int lastCommittedTransactionChecksum()
        {
            return 11;
        }

        @Override
        public long lastCommittedTransactionLogVersion()
        {
            return 12;
        }

        @Override
        public long lastCommittedTransactionLogByteOffset()
        {
            return 13;
        }
    }
}
