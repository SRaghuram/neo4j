/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Config;
import com.neo4j.causalclustering.stresstests.Control;
import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helper.Workload;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.kernel.diagnostics.utils.DumpUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.DaemonThreadFactory;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_listen_address;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
@ExtendWith( TestDirectoryExtension.class )
class BackupServiceStressTesting
{
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "8200";
    private static final String DEFAULT_TX_PRUNE = "50 files";

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        String backupHostname = fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_HOSTNAME", DEFAULT_HOSTNAME );
        int backupPort = parseInt( fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_PORT", DEFAULT_PORT ) );
        String txPrune = fromEnv( "BACKUP_SERVICE_STRESS_TX_PRUNE", DEFAULT_TX_PRUNE );

        File storeDir = testDirectory.storeDir( DEFAULT_DATABASE_NAME );
        File backupsDir = testDirectory.directory( "backups" );

        GraphDatabaseBuilder graphDatabaseBuilder = new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( online_backup_enabled, TRUE )
                .setConfig( online_backup_listen_address, SocketAddress.format( backupHostname, backupPort ) )
                .setConfig( keep_logical_logs, txPrune )
                .setConfig( logical_log_rotation_threshold, "1M" );

        Control control = new Control( new Config() );

        AtomicReference<GraphDatabaseService> dbRef = new AtomicReference<>();
        ExecutorService executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
        try
        {
            dbRef.set( graphDatabaseBuilder.newGraphDatabase() );

            TransactionalWorkload transactionalWorkload = new TransactionalWorkload( control, dbRef::get );
            BackupLoad backupWorkload = new BackupLoad( control, backupHostname, backupPort, backupsDir.toPath() );
            StartStop startStopWorkload = new StartStop( control, graphDatabaseBuilder::newGraphDatabase, dbRef );

            executeWorkloads( control, executor, asList( transactionalWorkload, backupWorkload, startStopWorkload ) );

            shutdown( executor );
        }
        catch ( TimeoutException t )
        {
            System.err.println( "Timeout waiting task completion. Dumping all threads." );
            printThreadDump();
            throw t;
        }
        finally
        {
            dbRef.get().shutdown();
            executor.shutdown();
        }
    }

    private static void executeWorkloads( Control control, ExecutorService executor, List<Workload> workloads ) throws Exception
    {
        for ( Workload workload : workloads )
        {
            workload.prepare();
        }

        List<Future<?>> futures = workloads.stream()
                .map( executor::submit )
                .collect( toList() );

        control.awaitEnd( futures );
        control.assertNoFailure();

        for ( Workload workload : workloads )
        {
            workload.validate();
        }
    }

    private static void shutdown( ExecutorService service ) throws InterruptedException
    {
        service.shutdown();
        if ( !service.awaitTermination( 30, SECONDS ) )
        {
            printThreadDump();
            fail( "Didn't manage to shut down the workers correctly, dumped threads for forensic purposes" );
        }
    }

    private static void printThreadDump()
    {
        System.err.println( DumpUtils.threadDump() );
    }
}
