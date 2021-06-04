/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectSpecificFormat;

@ServiceProvider
public class StandardBatchImporterFactory extends BatchImporterFactory
{
    public StandardBatchImporterFactory()
    {
        super( 1 );
    }

    @Override
    public String getName()
    {
        return "standard";
    }

    @Override
    public BatchImporter instantiate(DatabaseLayout directoryStructure, FileSystemAbstraction fileSystem, PageCache externalPageCache,
                                     PageCacheTracer pageCacheTracer, Configuration configuration, Log4jLogProvider logProvider,
                                     Config dbConfig, boolean verbose, JobScheduler jobScheduler, Collector badCollector,
                                     MemoryTracker memoryTracker, PrintStream stdOut, PrintStream stdErr) {
        return  instantiate( directoryStructure,
                fileSystem,
                externalPageCache,
                pageCacheTracer,
                configuration,
                logProvider,
                dbConfig,
                verbose,
                jobScheduler,
                badCollector,
                memoryTracker,
                stdOut,  stdErr, null);
    }

    /*@Override
    public BatchImporter instantiate( DatabaseLayout directoryStructure, FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer, Configuration config,
            LogService logService, ExecutionMonitor executionMonitor, AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats,
            ImportLogic.Monitor monitor, JobScheduler scheduler, Collector badCollector,
            LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker )
    {
        return new ParallelBatchImporter( directoryStructure, fileSystem, pageCacheTracer, config, logService, executionMonitor,
                additionalInitialIds, dbConfig, recordFormats, monitor, scheduler, badCollector, logFilesInitializer, memoryTracker );
    }*/
    @Override
    public ParallelBatchImporter instantiate(DatabaseLayout directoryStructure,
                                             FileSystemAbstraction fileSystem,
                                             PageCache externalPageCache,
                                             PageCacheTracer pageCacheTracer,
                                             Configuration config,
                                             Log4jLogProvider logProvider,
                                             Config dbConfig,
                                             boolean verbose,
                                             JobScheduler jobScheduler,
                                             Collector badCollector,
                                             MemoryTracker memoryTracker, PrintStream stdOut, PrintStream stdErr, String graphName )
    {
        ExecutionMonitor executionMonitor = verbose ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, stdOut,
                SpectrumExecutionMonitor.DEFAULT_WIDTH ) : ExecutionMonitors.defaultVisible();
        //PrintingImportLogicMonitor pLogicMonitor = new PrintingImportLogicMonitor(stdOut, stdErr);
        return new ParallelBatchImporter( directoryStructure,
                fileSystem,
                externalPageCache,
                pageCacheTracer,
                config,
                new SimpleLogService(NullLogProvider.getInstance(), logProvider),
                executionMonitor,
                EMPTY,
                dbConfig,
                directoryStructure.getDatabaseName().endsWith(".gds") ? selectSpecificFormat("CSR") : RecordFormatSelector.selectForConfig(dbConfig, logProvider),
                ImportLogic.NO_MONITOR,
                jobScheduler,
                badCollector,
                TransactionLogInitializer.getLogFilesInitializer(),
                memoryTracker, graphName );
    }
}
