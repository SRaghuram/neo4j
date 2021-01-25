/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.helper.StressTestingHelper.fromEnv;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ClusterExtension
class ClusterMultiDatabaseCreationStressTesting
{

    private static final String DEFAULT_DURATION_IN_MINUTES = "5";

    private static final String DEFAULT_NUM_THREADS = "10";

    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void multiDatabaseLifecycleStressTest() throws Exception
    {

        var durationInMinutes = parseInt( fromEnv( "CLUSTER_MULTIDATABASE_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        var threads = parseInt( fromEnv( "CLUSTER_MULTIDATABASE_STRESS_NUM_THREADS", DEFAULT_NUM_THREADS ) );

        var clusterConfig = clusterConfig()
                .withSharedCoreParam( EnterpriseEditionSettings.max_number_of_databases, String.valueOf( Long.MAX_VALUE ) )
                .withSharedCoreParam( GraphDatabaseSettings.preallocate_logical_logs, "false" )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 );

        Cluster cluster = startCluster( clusterConfig );

        ExecutorService executorPool = Executors.newFixedThreadPool( threads );

        try
        {
            executeMultiDatabaseCommands( durationInMinutes, threads, cluster, executorPool );
        }
        finally
        {
            cluster.shutdown();
            executorPool.shutdown();
        }
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws InterruptedException, ExecutionException
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );
        return cluster;
    }

    private static void executeMultiDatabaseCommands( int durationInMinutes, int threads, Cluster cluster, ExecutorService executorPool )
            throws InterruptedException
    {
        // Get the leader DBMS so that we can actually invoke commands on a given database
        Function<String,DatabaseManagementService> dbToLeaderDbms = database ->
        {
            try
            {
                return cluster.awaitCoreMemberWithRole( database, Role.LEADER, 5, TimeUnit.SECONDS )
                              .managementService();
            }
            catch ( TimeoutException e )
            {
                throw new RetrieveDbmsException( "Timeout out exception while retrieving leader of database " + database, e );
            }
        };

        long finishTimeMillis = System.currentTimeMillis() + MINUTES.toMillis( durationInMinutes );
        CountDownLatch executorLatch = new CountDownLatch( threads );
        List<CommandExecutor> commandExecutors = new ArrayList<>( threads );

        for ( int i = 0; i < threads; i++ )
        {
            CommandExecutor commandExecutor = new CommandExecutor( dbToLeaderDbms, executorLatch, finishTimeMillis );
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
