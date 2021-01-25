/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

@ServiceProvider
public class RestartableBatchImporterFactory extends BatchImporterFactory
{
    public RestartableBatchImporterFactory()
    {
        super( 10 );
    }

    @Override
    public String getName()
    {
        return "restartable";
    }

    @Override
    public BatchImporter instantiate( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            PageCacheTracer pageCacheTracer, Configuration config,
            LogService logService, ExecutionMonitor executionMonitor, AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats,
            ImportLogic.Monitor monitor, JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer,
            MemoryTracker memoryTracker )
    {
        return new RestartableParallelBatchImporter( databaseLayout, fileSystem, externalPageCache, pageCacheTracer, config, logService, executionMonitor,
                additionalInitialIds, dbConfig, recordFormats, monitor, jobScheduler, badCollector, logFilesInitializer, memoryTracker );
    }
}
