/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Config;
import com.neo4j.causalclustering.stresstests.Control;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.helper.Workload;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.utils.DumpUtils;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.DaemonThreadFactory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
@TestDirectoryExtension
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

        String databaseName = "testDatabase";

        Path storeDir = testDirectory.homePath( databaseName );
        Path backupsDir = testDirectory.directory( "backups" );

        DatabaseManagementServiceBuilder databaseManagementServiceBuilder =
                new EnterpriseDatabaseManagementServiceBuilder( storeDir )
                .setConfig( online_backup_enabled, true )
                .setConfig( auth_enabled, true )
                .setConfig( online_backup_listen_address, new SocketAddress( backupHostname, backupPort ) )
                .setConfig( keep_logical_logs, txPrune )
                .setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) );

        Control control = new Control( new Config() );

        AtomicReference<GraphDatabaseService> dbRef = new AtomicReference<>();
        ExecutorService executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
        DatabaseManagementService managementService = null;
        try
        {
            managementService = databaseManagementServiceBuilder.build();
            managementService.createDatabase( databaseName );
            dbRef.set( managementService.database( databaseName ) );

            TransactionalWorkload transactionalWorkload = new TransactionalWorkload( control, dbRef::get );
            BackupLoad backupWorkload = new BackupLoad( control, backupHostname, backupPort, backupsDir );
            StartStop startStopWorkload = new StartStop( control, dbRef, managementService, databaseName );

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
            if ( managementService != null )
            {
                managementService.shutdown();
            }
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
