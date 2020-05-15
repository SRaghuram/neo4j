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

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.BaseImportLogic.Monitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p>
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter
{
    private final PageCache externalPageCache;
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fileSystem;
    private final PageCacheTracer pageCacheTracer;
    private final Configuration config;
    private final LogService logService;
    private final Config dbConfig;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;
    private final Collector badCollector;
    private BaseImportLogic importLogic;
    private BatchingStoreBase store;
    private final LogFilesInitializer logFilesInitializer;
    private final MemoryTracker memoryTracker;

    public ParallelBatchImporter( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            PageCacheTracer pageCacheTracer, Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig,
            BaseImportLogic importLogic, BatchingStoreBase store,
            Monitor monitor, JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker)
    {
        this.externalPageCache = externalPageCache;
        this.databaseLayout = databaseLayout;
        this.fileSystem = fileSystem;
        this.pageCacheTracer = pageCacheTracer;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
        this.badCollector = badCollector;
        this.importLogic = importLogic;
        this.store = store;
        this.logFilesInitializer = logFilesInitializer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
            store.createNew();
            importLogic.initialize( input );
            importLogic.importNodes();
            importLogic.prepareIdMapper();
            importLogic.importRelationships();
            importLogic.calculateNodeDegrees();
            importLogic.buildInternalStructures();
            importLogic.buildCountsStore();
            logFilesInitializer.initializeLogFiles( dbConfig, databaseLayout, store, fileSystem );
            importLogic.success();
            store.close();
            importLogic.close();
    }
}
