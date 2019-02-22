/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport.store;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.global.GlobalMetricsExtension;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.configuration.Settings.TRUE;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class BatchingNeoStoresIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private File databaseDirectory;
    private AssertableLogProvider provider;
    private SimpleLogService logService;

    @BeforeEach
    void setUp()
    {
        databaseDirectory = testDirectory.databaseDir();
        provider = new AssertableLogProvider();
        logService = new SimpleLogService( provider, provider );
    }

    @Test
    void startBatchingNeoStoreWithMetricsPluginEnabled() throws Exception
    {
        Config config = Config.defaults( MetricsSettings.metricsEnabled, TRUE );
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, databaseDirectory, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, AdditionalInitialIds.EMPTY, config, jobScheduler ) )
        {
            batchingNeoStores.createNew();
        }
        provider.assertNone( AssertableLogProvider.inLog( GlobalMetricsExtension.class ).any() );
    }

    @Test
    void createStoreWithNotEmptyInitialIds() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores batchingNeoStores = BatchingNeoStores
                .batchingNeoStores( fileSystem, databaseDirectory, RecordFormatSelector.defaultFormat(), Configuration.DEFAULT,
                        logService, new TestAdditionalInitialIds(), Config.defaults(), jobScheduler ) )
        {
            batchingNeoStores.createNew();
        }

        GraphDatabaseService database = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
        try
        {
            TransactionIdStore transactionIdStore = getTransactionIdStore( (GraphDatabaseAPI) database );
            assertEquals( 10, transactionIdStore.getLastCommittedTransactionId() );
        }
        finally
        {
            database.shutdown();
        }
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
        public long lastCommittedTransactionChecksum()
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
