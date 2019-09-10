/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PageCacheWarmupEnterpriseEditionIT extends PageCacheWarmupTestSupport
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final EnterpriseDbmsRule db = new EnterpriseDbmsRule( testDirectory )
    {
        @Override
        protected void configure( DatabaseManagementServiceBuilder databaseFactory )
        {
            super.configure( databaseFactory );
            ((TestDatabaseManagementServiceBuilder) databaseFactory).setInternalLogProvider( logProvider );
        }
    }.startLazily();

    private static void verifyEventuallyWarmsUp( long pagesInMemory, File metricsDirectory ) throws Exception
    {
        assertEventually( "Metrics report should include page cache page faults",
                () -> readLongCounterValue( metricsCsv( metricsDirectory, "neo4j.page_cache.page_faults" ) ),
                greaterThanOrEqualTo( pagesInMemory ), 20, SECONDS );
    }

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, false )
          .withSetting( OnlineBackupSettings.online_backup_enabled, false )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        db.ensureStarted();

        createData();
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase( Map.of(
                MetricsSettings.metricsEnabled, true,
                MetricsSettings.csvEnabled, true,
                MetricsSettings.csvInterval, Duration.ofMillis( 100 ),
                MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath() ) );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    private void createData()
    {
        try ( var transaction = db.beginTx() )
        {
            createTestData( transaction );
            transaction.commit();
        }
    }

    @Test
    public void cacheProfilesMustBeIncludedInOnlineBackups() throws Exception
    {
        db.withSetting( MetricsSettings.metricsEnabled, false )
          .withSetting( OnlineBackupSettings.online_backup_enabled, true )
          .withSetting( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "localhost", 0 ) )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        db.ensureStarted();

        createData();
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        BinaryLatch latch = pauseProfile( db.getMonitors() ); // We don't want torn profile files in this test.

        File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
        File backupDir = testDirectory.cleanDirectory( "backup" );
        executeBackup( backupDir );
        latch.release();
        DbmsRule.RestartAction useBackupDir = ( fs, storeDir ) ->
        {
            fs.deleteRecursively( storeDir.databaseDirectory() );
            fs.copyRecursively( backupDir, storeDir.databaseDirectory() );
        };
        db.restartDatabase( useBackupDir, Map.of(
                OnlineBackupSettings.online_backup_enabled, false,
                MetricsSettings.metricsEnabled, true,
                MetricsSettings.csvEnabled, true,
                MetricsSettings.csvInterval, Duration.ofMillis( 100 ),
                MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath() ) );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void cacheProfilesMustNotInterfereWithOnlineBackups() throws Exception
    {
        // Here we are testing that the file modifications done by the page cache profiler,
        // does not make online backup throw any exceptions.
        db.withSetting( MetricsSettings.metricsEnabled, false )
          .withSetting( OnlineBackupSettings.online_backup_enabled, true )
          .withSetting( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "localhost", 0 ) )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, Duration.ofMillis( 1 ) );
        db.ensureStarted();

        createData();
        waitForCacheProfile( db.getMonitors() );

        for ( int i = 0; i < 20; i++ )
        {
            File backupDir = testDirectory.cleanDirectory( "backup" );
            executeBackup( backupDir );
        }
    }

    @Test
    public void cacheProfilesMustBeIncludedInOfflineBackups() throws Exception
    {
        File data = testDirectory.directory( "data" );
        File logs = new File( data, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        db.withSetting( MetricsSettings.metricsEnabled, false )
          .withSetting( OnlineBackupSettings.online_backup_enabled, false )
          .withSetting( GraphDatabaseSettings.databases_root_path, testDirectory.homeDir().toPath().toAbsolutePath() )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        db.ensureStarted();
        createData();
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.shutdown();

        File databaseDir = db.databaseLayout().databaseDirectory();
        File databases = new File( data, "databases" );
        File graphdb = testDirectory.databaseDir( databases );
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
        db.withSetting( MetricsSettings.metricsEnabled, true )
          .withSetting( MetricsSettings.csvEnabled, true )
          .withSetting( MetricsSettings.csvInterval, Duration.ofMillis( 100 ) )
          .withSetting( GraphDatabaseSettings.fail_on_missing_files, false )
          .withSetting( MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath() );
        db.ensureStarted();

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void logPageCacheWarmupStartCompletionMessages() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, false )
                .withSetting( OnlineBackupSettings.online_backup_enabled, false )
                .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, Duration.ofMillis( 100 ) );
        db.ensureStarted();

        createData();
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase( Map.of(
                MetricsSettings.metricsEnabled, true,
                MetricsSettings.csvEnabled, true,
                MetricsSettings.csvInterval, Duration.ofMillis( 100 ),
                MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath() ) );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );

        logProvider.rawMessageMatcher().assertContains( "Page cache warmup started." );
        logProvider.rawMessageMatcher().assertContains( "Page cache warmup completed. %d pages loaded. Duration: %s." );
    }

    @Test
    public void willPrefetchEverything() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );

        db.withSetting( MetricsSettings.metricsEnabled, false )
                .withSetting( OnlineBackupSettings.online_backup_enabled, false )
                .withSetting( GraphDatabaseSettings.pagecache_warmup_enabled, false )
                .withSetting( GraphDatabaseSettings.pagecache_memory, "50M" ) //enough to keep everything in page-cache & prevent evictions
                .ensureStarted();
        createData();

        db.restartDatabase();
        long pagesInMemoryWithoutPrefetch = getPageFaults( db );
        touchAllPages( db );
        long pagesInMemoryWithoutPrefetchAfterTouch = getPageFaults( db );

        Map<Setting<?>,Object> config = Map.of(
                GraphDatabaseSettings.pagecache_warmup_enabled, true,
                GraphDatabaseSettings.pagecache_warmup_prefetch, true,
                MetricsSettings.metricsEnabled, true,
                MetricsSettings.csvEnabled, true,
                MetricsSettings.csvInterval, Duration.ofMillis( 100 ),
                MetricsSettings.csvPath, metricsDirectory.toPath().toAbsolutePath()
        );

        db.restartDatabase( config );
        verifyEventuallyWarmsUp( pagesInMemoryWithoutPrefetchAfterTouch, metricsDirectory );

        long pagesInMemoryWithPrefetch = getPageFaults( db );
        touchAllPages( db );
        long pagesInMemoryWithPrefetchAfterTouch = getPageFaults( db );

        assertThat( pagesInMemoryWithoutPrefetch, lessThanOrEqualTo( pagesInMemoryWithoutPrefetchAfterTouch ) ); //we dont prefetch everything by default
        assertThat( pagesInMemoryWithoutPrefetchAfterTouch, lessThanOrEqualTo( pagesInMemoryWithPrefetch ) ); //prefetch should load same or more pages
        assertThat( pagesInMemoryWithPrefetch, equalTo( pagesInMemoryWithPrefetchAfterTouch ) ); //touching everything should not generate faults
    }

    private static void touchAllPages( EnterpriseDbmsRule db ) throws IOException
    {
        PageCache pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
        for ( PagedFile pagedFile : pageCache.listExistingMappings() )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                while ( cursor.next() )
                {
                    //do nothing
                }
            }
            pageCache.reportEvents();
        }
    }

    private static long getPageFaults( EnterpriseDbmsRule db )
    {
        return db.getDependencyResolver().resolveDependency( PageCacheTracer.class ).faults();
    }

    private void executeBackup( File backupDir ) throws Exception
    {
        HostnamePort address = db.resolveDependency( ConnectorPortRegister.class ).getLocalAddress( BACKUP_SERVER_NAME );

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( address.getHost(), address.getPort() )
                .withBackupDirectory( backupDir.toPath() )
                .withReportsDirectory( backupDir.toPath() )
                .build();

        OnlineBackupExecutor.buildDefault().executeBackup( context );
    }
}
