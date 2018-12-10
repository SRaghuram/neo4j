/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.neo4j.causalclustering.stresstests.Config;
import com.neo4j.causalclustering.stresstests.Control;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helper.Workload;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.diagnostics.utils.DumpUtils;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;
import static org.neo4j.helper.DatabaseConfiguration.configureBackup;
import static org.neo4j.helper.DatabaseConfiguration.configureTxLogRotationAndPruning;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class BackupServiceStressTesting
{
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "8200";
    private static final String DEFAULT_ENABLE_INDEXES = "false";
    private static final String DEFAULT_TX_PRUNE = "50 files";

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        String directory = fromEnv( "BACKUP_SERVICE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        String backupHostname = fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_HOSTNAME", DEFAULT_HOSTNAME );
        int backupPort = parseInt( fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_PORT", DEFAULT_PORT ) );
        String txPrune = fromEnv( "BACKUP_SERVICE_STRESS_TX_PRUNE", DEFAULT_TX_PRUNE );
        boolean enableIndexes =
                parseBoolean( fromEnv( "BACKUP_SERVICE_STRESS_ENABLE_INDEXES", DEFAULT_ENABLE_INDEXES ) );

        File store = new File( directory, "databases/graph.db" );
        File work = new File( directory, "backups/graph.db-backup" );
        FileUtils.deleteRecursively( store );
        FileUtils.deleteRecursively( work );
        File storeDirectory = ensureExistsAndEmpty( store );
        Path workDirectory = ensureExistsAndEmpty( work ).toPath();

        final Map<String,String> config =
                configureBackup( configureTxLogRotationAndPruning( new HashMap<>(), txPrune ), backupHostname,
                        backupPort );
        GraphDatabaseBuilder graphDatabaseBuilder = new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDirectory.getAbsoluteFile() )
                .setConfig( config );

        Control control = new Control( new Config() );

        AtomicReference<GraphDatabaseService> dbRef = new AtomicReference<>();
        ExecutorService service = Executors.newFixedThreadPool( 3 );
        try
        {
            dbRef.set( graphDatabaseBuilder.newGraphDatabase() );

            TransactionalWorkload transactionalWorkload = new TransactionalWorkload( control, dbRef::get, enableIndexes );
            BackupLoad backupWorkload = new BackupLoad( control, backupHostname, backupPort, workDirectory );
            StartStop startStopWorkload = new StartStop( control, graphDatabaseBuilder::newGraphDatabase, dbRef );

            executeWorkloads( control, service, asList( transactionalWorkload, backupWorkload, startStopWorkload ) );

            service.shutdown();
            if ( !service.awaitTermination( 30, SECONDS ) )
            {
                printThreadDump();
                fail( "Didn't manage to shut down the workers correctly, dumped threads for forensic purposes" );
            }
        }
        catch ( TimeoutException t )
        {
            System.err.println( format( "Timeout waiting task completion. Dumping all threads." ) );
            printThreadDump();
            throw t;
        }
        finally
        {
            dbRef.get().shutdown();
            service.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( storeDirectory );
        FileUtils.deletePathRecursively( workDirectory );
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

    private static void printThreadDump()
    {
        System.err.println( DumpUtils.threadDump() );
    }
}
