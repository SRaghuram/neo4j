/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.rule.CommercialDbmsRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

//import static org.neo4j.metrics.source.db.PageCacheMetrics.PC_PAGE_FAULTS;

public class PageCacheWarmupEnterpriseEditionIT extends PageCacheWarmupTestSupport
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final CommercialDbmsRule db = new CommercialDbmsRule( testDirectory )
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            super.configure( databaseFactory );
            ((TestGraphDatabaseFactory) databaseFactory).setInternalLogProvider( logProvider );
        }
    }.startLazily();

    private static void verifyEventuallyWarmsUp( long pagesInMemory, File metricsDirectory ) throws Exception
    {
//        assertEventually( "Metrics report should include page cache page faults",
//                () -> readLongCounterValue( metricsCsv( metricsDirectory, PC_PAGE_FAULTS ) ),
//                greaterThanOrEqualTo( pagesInMemory ), 20, SECONDS );
    }

    @Test
    public void warmupMustReloadHotPagesAfterRestartAndFaultsMustBeVisibleViaMetrics() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void cacheProfilesMustBeIncludedInOnlineBackups() throws Exception
    {
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( UdcSettings.udc_enabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .withSetting( OnlineBackupSettings.online_backup_listen_address, "localhost:0" )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
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
        db.restartDatabase( useBackupDir,
                OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE,
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void cacheProfilesMustNotInterfereWithOnlineBackups() throws Exception
    {
        // Here we are testing that the file modifications done by the page cache profiler,
        // does not make online backup throw any exceptions.
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
          .withSetting( OnlineBackupSettings.online_backup_listen_address, "localhost:0" )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "1ms" );
        db.ensureStarted();

        createTestData( db );
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
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
          .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
          .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();
        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.shutdownAndKeepStore();

        AdminTool adminTool = new AdminTool(
                CommandLocator.fromServiceLocator(),
                new RealOutsideWorld()
                {
                    @Override
                    public void exit( int status )
                    {
                        assertThat( "exit code", status, is( 0 ) );
                    }
                },
                true );
        File databaseDir = db.databaseLayout().databaseDirectory();
        File data = testDirectory.cleanDirectory( "data" );
        File databases = new File( data, "databases" );
        File logs = new File( data, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        File graphdb = testDirectory.databaseDir( databases );
        FileUtils.copyRecursively( databaseDir, graphdb );
        deleteRecursively( databaseDir );
        Path homePath = data.toPath().getParent();
        File dumpDir = testDirectory.cleanDirectory( "dump-dir" );
        adminTool.execute( homePath, homePath, "dump", "--database=" + DEFAULT_DATABASE_NAME, "--to=" + dumpDir );

        deleteRecursively( graphdb );
        cleanDirectory( logs );
        File dumpFile = new File( dumpDir, "graph.db.dump" );
        adminTool.execute( homePath, homePath, "load", "--database=" + DEFAULT_DATABASE_NAME, "--from=" + dumpFile );
        FileUtils.copyRecursively( graphdb, databaseDir );
        deleteRecursively( graphdb );

        File metricsDirectory = testDirectory.cleanDirectory( "metrics" );
        db.withSetting( MetricsSettings.neoPageCacheEnabled, Settings.TRUE )
          .withSetting( MetricsSettings.csvEnabled, Settings.TRUE )
          .withSetting( MetricsSettings.csvInterval, "100ms" )
          .withSetting( MetricsSettings.csvPath, metricsDirectory.getAbsolutePath() );
        db.ensureStarted();

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );
    }

    @Test
    public void logPageCacheWarmupStartCompletionMessages() throws Exception
    {
        File metricsDirectory = testDirectory.directory( "metrics" );
        db.withSetting( MetricsSettings.metricsEnabled, Settings.FALSE )
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withSetting( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );
        db.ensureStarted();

        createTestData( db );
        long pagesInMemory = waitForCacheProfile( db.getMonitors() );

        db.restartDatabase(
                MetricsSettings.neoPageCacheEnabled.name(), Settings.TRUE,
                MetricsSettings.csvEnabled.name(), Settings.TRUE,
                MetricsSettings.csvInterval.name(), "100ms",
                MetricsSettings.csvPath.name(), metricsDirectory.getAbsolutePath() );

        verifyEventuallyWarmsUp( pagesInMemory, metricsDirectory );

        logProvider.assertContainsMessageContaining( "Page cache warmup started." );
        logProvider.assertContainsMessageContaining( "Page cache warmup completed. %d pages loaded. Duration: %s." );
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
