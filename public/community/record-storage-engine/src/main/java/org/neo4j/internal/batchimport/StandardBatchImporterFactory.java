/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.importer.PrintingImportLogicMonitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.batchimport.BaseImportLogic.Monitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;

import java.io.IOException;
import java.io.PrintStream;

@ServiceProvider
public class StandardBatchImporterFactory extends BatchImporterFactory
{
    public StandardBatchImporterFactory()
    {
        super( 1 );
    }

    private final PrintStream stdOut = null;
    private final PrintStream stdErr = null;
    @Override
    public String getName()
    {
        return "standard";
    }

    @Override
    public BatchImporter instantiate(DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache externalPageCache,
                                     PageCacheTracer pageCacheTracer, Configuration config, LogService logService, ExecutionMonitor executionMonitor,
                                     AdditionalInitialIds additionalInitialIds, Config dbConfig, Monitor monitor, JobScheduler jobScheduler,
                                     Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker) throws IOException {

        BatchingNeoStores store = instantiateNeoStores( fileSystem, databaseLayout,
                externalPageCache, pageCacheTracer, config, logService, additionalInitialIds, dbConfig, jobScheduler, memoryTracker );
        ImportLogic importLogic = new ImportLogic( fileSystem, databaseLayout, store, config, dbConfig, logService,
                executionMonitor, badCollector, monitor, pageCacheTracer, memoryTracker );

        BatchImporter batchImporter = new ParallelBatchImporter(databaseLayout, fileSystem, externalPageCache, pageCacheTracer, config,
                logService, executionMonitor, additionalInitialIds,
                dbConfig, importLogic, store,
                new PrintingImportLogicMonitor(stdOut, stdErr), jobScheduler, badCollector, logFilesInitializer, memoryTracker);
        return batchImporter;
    }

    public static BatchingNeoStores instantiateNeoStores( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
                                                          PageCache externalPageCache, PageCacheTracer cacheTracer, Configuration config,
                                                          LogService logService, AdditionalInitialIds additionalInitialIds, Config dbConfig, JobScheduler scheduler, MemoryTracker memoryTracker )
    {
        RecordFormats recordFormats = RecordFormatSelector.selectForConfig(fileSystem, dbConfig);
        if ( externalPageCache == null )
        {
            return BatchingNeoStores.batchingNeoStores( fileSystem, databaseLayout, recordFormats, config, logService,
                    additionalInitialIds, dbConfig, scheduler, cacheTracer, memoryTracker );
        }

        return BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, externalPageCache,
                cacheTracer, databaseLayout, recordFormats, config, logService, additionalInitialIds, dbConfig, memoryTracker );
    }

}
