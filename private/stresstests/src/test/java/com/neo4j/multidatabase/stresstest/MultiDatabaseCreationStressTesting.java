/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest;

import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;

import static com.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

class MultiDatabaseCreationStressTesting
{
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_DURATION_IN_MINUTES = "5";
    private static final String DEFAULT_NUM_THREADS = "10";

    @Test
    void multiDatabaseLifecycleStressTest() throws IOException, InterruptedException
    {
        String workingDirectory = fromEnv( "MULTIDATABASE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        int durationInMinutes = parseInt( fromEnv( "MULTIDATABASE_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        int threads = parseInt( fromEnv( "MULTIDATABASE_STRESS_NUM_THREADS", DEFAULT_NUM_THREADS ) );
        File storeDirectory = new File( workingDirectory, "default" );

        deleteRecursively( storeDirectory );
        ensureExistsAndEmpty( storeDirectory );

        DatabaseManagementService managementService = new EnterpriseDatabaseManagementServiceBuilder( storeDirectory )
                .setConfig( EnterpriseEditionSettings.max_number_of_databases, Long.MAX_VALUE )
                .setConfig( GraphDatabaseSettings.preallocate_logical_logs, false )
                .build();
        ExecutorService executorPool = Executors.newFixedThreadPool( threads );

        try
        {
            executeMultiDatabaseCommands( durationInMinutes, threads, managementService, executorPool );
        }
        finally
        {
            managementService.shutdown();
            executorPool.shutdown();
        }
    }

    private static void executeMultiDatabaseCommands( int durationInMinutes, int threads, DatabaseManagementService dbms, ExecutorService executorPool )
            throws InterruptedException
    {
        long finishTimeMillis = System.currentTimeMillis() + MINUTES.toMillis( durationInMinutes );
        CountDownLatch executorLatch = new CountDownLatch( threads );
        List<CommandExecutor> commandExecutors = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            CommandExecutor commandExecutor = new CommandExecutor( dbms, executorLatch, finishTimeMillis );
            commandExecutors.add( commandExecutor );
            executorPool.submit( commandExecutor );
        }
        executorLatch.await();
        for ( CommandExecutor commandExecutor : commandExecutors )
        {
            commandExecutor.checkExecutionResults();
        }
    }
}
