/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint;

import com.neo4j.kernel.stresstests.transaction.checkpoint.tracers.TimerTransactionTracer;
import com.neo4j.kernel.stresstests.transaction.checkpoint.workload.Workload;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static com.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutationFactory.defaultRandomMutation;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
class CheckPointingLogRotationStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "5";
    private static final String DEFAULT_STORE_DIR = new File( getProperty( "java.io.tmpdir" ), "store" ).getPath();
    private static final String DEFAULT_NODE_COUNT = "100000";
    private static final String DEFAULT_WORKER_THREADS = "16";
    private static final String DEFAULT_PAGE_CACHE_MEMORY = "4g";

    private static final int CHECK_POINT_INTERVAL_MINUTES = 1;

    @Test
    void shouldBehaveCorrectlyUnderStress() throws Throwable
    {
        long durationInMinutes =
                parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        File storeDir = new File( fromEnv( "CHECK_POINT_LOG_ROTATION_STORE_DIRECTORY", DEFAULT_STORE_DIR ) );
        long nodeCount = parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_NODE_COUNT", DEFAULT_NODE_COUNT ) );
        int threads = parseInt( fromEnv( "CHECK_POINT_LOG_ROTATION_WORKER_THREADS", DEFAULT_WORKER_THREADS ) );
        String pageCacheMemory = fromEnv( "CHECK_POINT_LOG_ROTATION_PAGE_CACHE_MEMORY", DEFAULT_PAGE_CACHE_MEMORY );

        System.out.println( "1/6\tBuilding initial store..." );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            Config dbConfig = Config.defaults();
            new ParallelBatchImporter(
                    DatabaseLayout.ofFlat( ensureExistsAndEmpty( storeDir ) ), fileSystem, null, PageCacheTracer.NULL,
                    DEFAULT, NullLogService.getInstance(), ExecutionMonitors.defaultVisible(), EMPTY, dbConfig,
                    RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() ), NO_MONITOR, jobScheduler,
                    Collector.EMPTY, TransactionLogInitializer.getLogFilesInitializer(), INSTANCE )
                    .doImport( new NodeCountInputs( nodeCount ) );
        }

        System.out.println( "2/6\tStarting database..." );
        DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( storeDir );
        DatabaseManagementService managementService = builder
                .setConfig( GraphDatabaseSettings.pagecache_memory, pageCacheMemory )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, FALSE )
                .setConfig( GraphDatabaseSettings.check_point_interval_time, Duration.ofMinutes( CHECK_POINT_INTERVAL_MINUTES ) )
                .setConfig( GraphDatabaseInternalSettings.tracer, "timer" ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        System.out.println("3/6\tWarm up db...");
        try ( Workload workload = new Workload( db, defaultRandomMutation( nodeCount, db ), threads ) )
        {
            // make sure to run at least one checkpoint during warmup
            long warmUpTimeMillis = TimeUnit.SECONDS.toMillis( CHECK_POINT_INTERVAL_MINUTES * 2 );
            workload.run( warmUpTimeMillis, Workload.TransactionThroughput.NONE );
        }

        System.out.println( "4/6\tStarting workload..." );
        TransactionThroughputChecker throughput = new TransactionThroughputChecker();
        try ( Workload workload = new Workload( db, defaultRandomMutation( nodeCount, db ), threads ) )
        {
            workload.run( TimeUnit.MINUTES.toMillis( durationInMinutes ), throughput );
        }

        System.out.println( "5/6\tShutting down..." );
        managementService.shutdown();

        try
        {
            System.out.println( "6/6\tPrinting stats and recorded timings..." );
            TimerTransactionTracer.printStats( System.out );
            throughput.assertThroughput( System.out );
        }
        finally
        {
            System.out.println( "Done." );
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( storeDir );
    }
}
