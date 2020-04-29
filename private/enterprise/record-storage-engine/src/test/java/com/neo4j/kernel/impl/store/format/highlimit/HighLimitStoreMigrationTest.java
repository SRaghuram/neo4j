/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import java.io.File;
import java.io.IOException;

import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.StandardBatchImporterFactory;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore35;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@PageCacheExtension
@Neo4jLayoutExtension
class HighLimitStoreMigrationTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private DatabaseLayout databaseLayout;
    private final BatchImporterFactory batchImporterFactory = BatchImporterFactory.withHighestPriority();

    @Test
    void haveDifferentFormatCapabilitiesAsHighLimit3_0()
    {
        assertFalse( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_0_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    void haveSameFormatCapabilitiesAsHighLimit3_4()
    {
        assertTrue( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_4_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    void migrateHighLimit3_0StoreFiles() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            RecordStorageMigrator migrator = new RecordStorageMigrator( fileSystem, pageCache, Config.defaults(), NullLogService.getInstance(), jobScheduler,
                    NULL, batchImporterFactory, INSTANCE );
            DatabaseLayout migrationLayout = neo4jLayout.databaseLayout( "migration" );
            fileSystem.mkdirs( migrationLayout.databaseDirectory() );

            prepareStoreFiles( fileSystem, databaseLayout, HighLimitV3_0_0.STORE_VERSION, pageCache );

            ProgressReporter progressMonitor = mock( ProgressReporter.class );

            migrator.migrate( databaseLayout, migrationLayout, progressMonitor, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

            int newStoreFilesCount = fileSystem.listFiles( migrationLayout.databaseDirectory() ).length;
            assertThat( newStoreFilesCount ).as( "Store should be migrated and new store files should be created." ).isGreaterThanOrEqualTo(
                    StoreType.values().length );
        }
    }

    @Test
    void settingForNumberOfProcessorsToUseWhenMigratingShouldPropagateToBatchImporter() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            Config config = Config.defaults();
            int maxNbrOfProcessors = 2;
            config.set( GraphDatabaseSettings.upgrade_processors, maxNbrOfProcessors );
            TrackingBatchImporterFactory batchImporterFactory = new TrackingBatchImporterFactory();

            RecordStorageMigrator migrator = new RecordStorageMigrator( fileSystem, pageCache, config, NullLogService.getInstance(), jobScheduler,
                    NULL, batchImporterFactory, INSTANCE );
            DatabaseLayout migrationLayout = neo4jLayout.databaseLayout( "migration" );
            fileSystem.mkdirs( migrationLayout.databaseDirectory() );

            prepareStoreFiles( fileSystem, databaseLayout, HighLimitV3_0_0.STORE_VERSION, pageCache );

            migrator.migrate( databaseLayout, migrationLayout, ProgressReporter.SILENT, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

            assertThat( batchImporterFactory.configuration.maxNumberOfProcessors() ).isEqualTo( maxNbrOfProcessors );
        }
    }

    private static void prepareStoreFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, String storeVersion, PageCache pageCache )
            throws IOException
    {
        File neoStoreFile = createNeoStoreFile( fileSystem, databaseLayout );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value, PageCursorTracer.NULL );
        createSchemaStoreFile( fileSystem, databaseLayout, pageCache );
    }

    private static File createNeoStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        fileSystem.write( neoStoreFile ).close();
        return neoStoreFile;
    }

    private static void createSchemaStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        File store = databaseLayout.schemaStore();
        File idFile = databaseLayout.idSchemaStore();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        RecordFormats recordFormats = HighLimitV3_0_0.RECORD_FORMATS;
        Config config = Config.defaults();
        IdType idType = IdType.SCHEMA;
        try ( SchemaStore35 schemaStore35 = new SchemaStore35( store, idFile, config, idType, idGeneratorFactory, pageCache, logProvider, recordFormats,
                immutable.empty() ) )
        {
            schemaStore35.initialise( true, PageCursorTracer.NULL );
        }
    }

    private static class TrackingBatchImporterFactory extends BatchImporterFactory
    {
        private StandardBatchImporterFactory delegate;
        private Configuration configuration;

        TrackingBatchImporterFactory()
        {
            super( Integer.MAX_VALUE );
            delegate = new StandardBatchImporterFactory();
        }

        @Override
        public BatchImporter instantiate(
                DatabaseLayout directoryStructure, FileSystemAbstraction fileSystem, PageCache externalPageCache,
                PageCacheTracer pageCacheTracer, Configuration config, LogService logService, ExecutionMonitor executionMonitor,
                AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats, ImportLogic.Monitor monitor, JobScheduler jobScheduler,
                Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker )
        {
            this.configuration = config;
            return delegate
                    .instantiate( directoryStructure, fileSystem, externalPageCache, pageCacheTracer, config, logService, executionMonitor,
                            additionalInitialIds, dbConfig, recordFormats, monitor, jobScheduler, badCollector, logFilesInitializer, memoryTracker );
        }

        @Override
        public String getName()
        {
            return "TrackingBatchImporterFactory";
        }
    }
}
