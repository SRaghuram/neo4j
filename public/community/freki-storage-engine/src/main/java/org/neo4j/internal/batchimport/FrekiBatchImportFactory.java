package org.neo4j.internal.batchimport;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import java.io.IOException;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

@ServiceProvider
public class FrekiBatchImportFactory extends BatchImporterFactory
    {

        DatabaseLayout databaseLayout;
        FileSystemAbstraction fileSystem;
        PageCache pageCache;
        Configuration config;
        LogService logService;
        ExecutionMonitor executionMonitor;
        AdditionalInitialIds additionalInitialIds;
        Config dbConfig;
        BaseImportLogic.Monitor monitor;
        JobScheduler jobScheduler;
        Collector badCollector;
        LogFilesInitializer logFilesInitializer;
        IdGeneratorFactory idGeneratorFactory;
    public FrekiBatchImportFactory()
        {
            super( 1 );
        }
    @Override
    public BatchImporter instantiate(DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem,
                                     PageCache externalPageCache, PageCacheTracer pageCacheTracer, Configuration config,
                                     LogService logService, ExecutionMonitor executionMonitor, AdditionalInitialIds additionalInitialIds,
                                     Config dbConfig, BaseImportLogic.Monitor monitor, JobScheduler jobScheduler,
                                     Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker) throws IOException {
        this.databaseLayout = databaseLayout;
        this.fileSystem = fileSystem;
        if (externalPageCache == null)
        {
            SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( fileSystem );
            pageCache = new MuninnPageCache( swapperFactory,
                    Integer.parseInt( dbConfig.get( pagecache_memory ) == null? "10000000": dbConfig.get( pagecache_memory )), new DefaultPageCacheTracer(),
                    EmptyVersionContextSupplier.EMPTY, jobScheduler );
        }
        else
            pageCache = externalPageCache;
        this.config = config;
        this.logService = logService;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.dbConfig = dbConfig;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
        this.badCollector = badCollector;
        this.logFilesInitializer = null;
        this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );

        FrekiBatchStores frekiStore =  new FrekiBatchStores( fileSystem, databaseLayout, idGeneratorFactory, pageCache, pageCacheTracer);
        frekiStore.initialize(dbConfig, idGeneratorFactory, logService.getInternalLogProvider(),
                pageCacheTracer, memoryTracker, immutable.empty());
        FrekiImportLogic frekiImportLogic = new FrekiImportLogic(fileSystem, databaseLayout, frekiStore, badCollector,
                logService,
                executionMonitor,
                config, dbConfig, monitor, pageCacheTracer,  memoryTracker);
        return new ParallelBatchImporter(databaseLayout, fileSystem, externalPageCache, pageCacheTracer,
                config, logService, executionMonitor,
                additionalInitialIds, dbConfig,
                frekiImportLogic, frekiStore,
                monitor, jobScheduler,
                badCollector, logFilesInitializer, memoryTracker);
    }

        @Override
        public String getName() {
            return "FrekiBatchImportFactory";
        }
}
