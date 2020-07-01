/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.MetricsSettings.csv_enabled;
import static com.neo4j.configuration.MetricsSettings.csv_interval;
import static com.neo4j.configuration.MetricsSettings.csv_path;
import static com.neo4j.configuration.MetricsSettings.metrics_enabled;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_profiling_interval;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( SuppressOutputExtension.class )
@Execution( CONCURRENT )
public class PageCacheWarmupEnterpriseEditionIT extends PageCacheWarmupTestSupport
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class WarmupReload
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseService db;
        @Inject
        private DbmsController controller;
        @Inject
        private Monitors monitors;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setConfig( metrics_enabled, false )
                   .setConfig( online_backup_enabled, false )
                   .setConfig( pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        }

        @Test
        void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics()
        {
            File metricsDirectory = testDirectory.directory( "metrics" );

            createData( db );
            long pagesInMemory = waitForCacheProfile( monitors );

            controller.restartDbms( builder ->
                            builder.setConfig( metrics_enabled, true )
                                   .setConfig( csv_enabled, true )
                                   .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                                   .setConfig( csv_path, metricsDirectory.toPath().toAbsolutePath() ) );

            verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
        }
    }

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class RepeatedOnlineBackupsCacheProfiles
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseAPI db;
        @Inject
        private Monitors monitors;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setConfig( metrics_enabled, false )
                    .setConfig( online_backup_enabled, true )
                    .setConfig( pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) )
                    .setConfig( online_backup_listen_address, new SocketAddress( "localhost", 0 ) );
        }

        @Test
        void cacheProfilesMustNotInterfereWithOnlineBackups() throws Exception
        {
            // Here we are testing that the file modifications done by the page cache profiler,
            // does not make online backup throw any exceptions.

            createData( db );
            waitForCacheProfile( monitors );

            for ( int i = 0; i < 5; i++ )
            {
                File backupDir = testDirectory.cleanDirectory( "backup" );
                executeBackup( db, backupDir );
            }
        }
    }

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class OnlineBackupsCacheProfiles
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseAPI db;
        @Inject
        private FileSystemAbstraction fs;
        @Inject
        private DatabaseLayout databaseLayout;
        @Inject
        private DbmsController controller;
        @Inject
        private Monitors monitors;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setConfig( metrics_enabled, false )
                   .setConfig( online_backup_enabled, true )
                   .setConfig( pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) )
                   .setConfig( online_backup_listen_address, new SocketAddress( "localhost", 0 ) );
        }

        @Test
        void cacheProfilesMustBeIncludedInOnlineBackups() throws Exception
        {
            createData( db );
            long pagesInMemory = waitForCacheProfile( monitors );

            BinaryLatch latch = pauseProfile( monitors ); // We don't want torn profile files in this test.

            File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
            File backupDir = testDirectory.cleanDirectory( "backup" );
            executeBackup( db, backupDir );
            latch.release();
            controller.restartDbms( builder ->
            {
                cleanupDirectories( backupDir );
                return builder.setConfig( online_backup_enabled, false )
                        .setConfig( metrics_enabled, true )
                        .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                        .setConfig( csv_path, metricsDirectory.toPath().toAbsolutePath() )
                        .setConfig( csv_enabled, true );
            } );

            verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
        }

        private void cleanupDirectories( File backupDir )
        {
            try
            {
                fs.deleteRecursively( databaseLayout.databaseDirectory().toFile() );
                fs.copyRecursively( backupDir, databaseLayout.getNeo4jLayout().databasesDirectory().toFile() );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class OfflineBackupsCacheProfiles
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseAPI db;
        @Inject
        private DatabaseManagementService dbms;
        @Inject
        private Monitors monitors;
        @Inject
        private DbmsController controller;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setConfig( metrics_enabled, false )
                    .setConfig( online_backup_enabled, false )
                    .setConfig( databases_root_path, testDirectory.homePath().toAbsolutePath() )
                    .setConfig( pagecache_warmup_profiling_interval, Duration.ofMillis( 100 )  );
        }

        @Test
        void cacheProfilesMustBeIncludedInOfflineBackups() throws Exception
        {
            File data = testDirectory.directory( "data" );
            File logs = new File( data, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
            createData( db );
            long pagesInMemory = waitForCacheProfile( monitors );

            dbms.shutdown();

            File databaseDir = db.databaseLayout().databaseDirectory().toFile();
            File databases = new File( data, "databases" );
            File graphdb = new File( databases, "neo4j" );
            FileUtils.copyRecursively( databaseDir, graphdb );
            deleteRecursively( databaseDir );
            Path homePath = data.toPath().getParent();
            File dumpDir = testDirectory.cleanDirectory( "dump-dir" );

            ExecutionContext ctx = new ExecutionContext( homePath, homePath, System.out, System.err, testDirectory.getFileSystem() );
            AdminTool.execute( ctx, "dump", "--database=" + DEFAULT_DATABASE_NAME, "--to=" + dumpDir );
            deleteRecursively( graphdb );
            cleanDirectory( logs );
            File dumpFile = new File( dumpDir, "neo4j.dump" );
            AdminTool.execute( ctx, "load", "--database=" + DEFAULT_DATABASE_NAME, "--from=" + dumpFile );
            FileUtils.copyRecursively( graphdb, databaseDir );
            deleteRecursively( graphdb );

            File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
            controller.restartDbms( builder ->
                    builder.setConfig( metrics_enabled, true )
                           .setConfig( csv_enabled, true )
                           .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                           .setConfig( fail_on_missing_files, false )
                           .setConfig( csv_path, metricsDirectory.toPath().toAbsolutePath() ) );

            verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
        }
    }

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class CacheProfilesWarmupMessages
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseAPI db;
        @Inject
        private Monitors monitors;
        @Inject
        private DbmsController controller;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setInternalLogProvider( logProvider )
                   .setConfig( metrics_enabled, false )
                   .setConfig( online_backup_enabled, false )
                   .setConfig( pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        }

        @Test
        void logPageCacheWarmupStartCompletionMessages()
        {
            File metricsDirectory = testDirectory.directory( "metrics" );

            createData( db );
            long pagesInMemory = waitForCacheProfile( monitors );

            controller.restartDbms( builder ->
                    builder.setConfig( metrics_enabled, true )
                           .setConfig( csv_enabled, true )
                           .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                           .setConfig( csv_path, metricsDirectory.toPath().toAbsolutePath() ) );

            verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );

            assertThat( logProvider ).containsMessages( "Page cache warmup started.", "Page cache warmup completed. %d pages loaded. Duration: %s." );
        }
    }

    @Nested
    @SkipThreadLeakageGuard
    @EnterpriseDbmsExtension( configurationCallback = "configure" )
    class CacheProfilesPrefetch
    {
        @Inject
        private TestDirectory testDirectory;
        @Inject
        private GraphDatabaseAPI db;
        @Inject
        private DbmsController controller;

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            builder.setConfig( metrics_enabled, false )
                   .setConfig( online_backup_enabled, false )
                    .setConfig( pagecache_memory, "50M" )
                    .setConfig( pagecache_warmup_enabled, false );
        }

        @Test
        void willPrefetchEverything() throws Exception
        {
            File metricsDirectory = testDirectory.directory( "metrics" );

            createData( db );

            controller.restartDbms();
            var pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
            var pageCacheTracer = db.getDependencyResolver().resolveDependency( PageCacheTracer.class );
            long pagesInMemoryWithoutPrefetch = pageCacheTracer.faults();
            touchAllPages( pageCache, pageCacheTracer );
            long pagesInMemoryWithoutPrefetchAfterTouch = pageCacheTracer.faults();

            controller.restartDbms( builder ->
                    builder.setConfig( pagecache_warmup_enabled, true )
                           .setConfig( pagecache_warmup_prefetch, true )
                           .setConfig( csv_enabled, true )
                           .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                           .setConfig( csv_path, metricsDirectory.toPath().toAbsolutePath() )
                           .setConfig( metrics_enabled, true ) );
            verifyEventuallyWarmsUp( pagesInMemoryWithoutPrefetchAfterTouch, metricsDirectory );

            pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
            pageCacheTracer = db.getDependencyResolver().resolveDependency( PageCacheTracer.class );
            long pagesInMemoryWithPrefetch = pageCacheTracer.faults();
            touchAllPages( pageCache, pageCacheTracer );
            long pagesInMemoryWithPrefetchAfterTouch = pageCacheTracer.faults();

            assertThat( pagesInMemoryWithoutPrefetch ).isLessThanOrEqualTo( pagesInMemoryWithoutPrefetchAfterTouch ); //we dont prefetch everything by default
            assertThat( pagesInMemoryWithoutPrefetchAfterTouch ).isLessThanOrEqualTo( pagesInMemoryWithPrefetch ); //prefetch should load same or more pages
            assertThat( pagesInMemoryWithPrefetch ).isEqualTo( pagesInMemoryWithPrefetchAfterTouch ); //touching everything should not generate faults
        }
    }

    private static void touchAllPages( PageCache pageCache, PageCacheTracer pageCacheTracer ) throws IOException
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "touchAll" ) )
        {
            for ( PagedFile pagedFile : pageCache.listExistingMappings() )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
                {
                    while ( cursor.next() )
                    {
                        //do nothing
                    }
                }
            }
        }
    }

    private static void verifyEventuallyWarmsUp( long pagesInMemory, File metricsDirectory )
    {
        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongCounterValue( metricsCsv( metricsDirectory, "neo4j.page_cache.page_faults" ) ), v -> v >= pagesInMemory, 20, SECONDS );
    }

    private static void createData( GraphDatabaseService db )
    {
        try ( var transaction = db.beginTx() )
        {
            createTestData( transaction );
            transaction.commit();
        }
    }

    private static void executeBackup( GraphDatabaseAPI db, File backupDir ) throws Exception
    {
        HostnamePort address = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class ).getLocalAddress( BACKUP_SERVER_NAME );

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( address.getHost(), address.getPort() )
                .withBackupDirectory( backupDir.toPath() )
                .withReportsDirectory( backupDir.toPath() )
                .build();

        OnlineBackupExecutor.buildDefault().executeBackup( context );
    }
}
