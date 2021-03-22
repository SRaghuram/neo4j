/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.causalclustering.TestAllClusterTypes;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.forceTxLogRotationAndCheckpoint;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ClusterExtension
@TestInstance( PER_METHOD )
class ReadReplicaStoreCopyIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    void start( ClusterConfig.ClusterType type ) throws Exception
    {
        var clusterConfig = clusterConfig()
                .withClusterType( type )
                .withSharedPrimaryParam( GraphDatabaseSettings.keep_logical_logs, FALSE )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.start( clusterConfig );
    }

    @TestAllClusterTypes
    @Timeout( 240 )
    void shouldNotBePossibleToStartTransactionsWhenReadReplicaCopiesStore( ClusterConfig.ClusterType clusterType ) throws Throwable
    {
        start( clusterType );
        var readReplica = cluster.findAnyReadReplica();

        var catchupPollingProcess = readReplica.resolveDependency( DEFAULT_DATABASE_NAME, CatchupPollingProcess.class );
        catchupPollingProcess.stop();

        writeSomeDataAndForceLogRotations( cluster );
        var storeCopyBlockingSemaphore = addStoreCopyBlockingMonitor( readReplica );
        try
        {
            catchupPollingProcess.start();
            waitForStoreCopyToStartAndBlock( storeCopyBlockingSemaphore );

            var replicaGraphDatabase = readReplica.defaultDatabase();
            assertThrows( DatabaseShutdownException.class, replicaGraphDatabase::beginTx );
        }
        finally
        {
            // release all waiters of the semaphore
            storeCopyBlockingSemaphore.release( Integer.MAX_VALUE );
        }
    }

    private static void writeSomeDataAndForceLogRotations( Cluster cluster ) throws Exception
    {
        for ( int i = 0; i < 20; i++ )
        {
            cluster.primaryTx( ( db, tx ) ->
            {
                tx.execute( "CREATE ()" );
                tx.commit();
            } );

            forceLogRotationOnAllCores( cluster );
        }
    }

    private static void forceLogRotationOnAllCores( Cluster cluster ) throws IOException
    {
        for ( CoreClusterMember core : cluster.primaryMembers() )
        {
            forceTxLogRotationAndCheckpoint( core.defaultDatabase() );
        }
    }

    private static Semaphore addStoreCopyBlockingMonitor( ReadReplica readReplica )
    {
        Semaphore semaphore = new Semaphore( 0 );

        readReplica.monitors().addMonitorListener( (FileCopyMonitor) file ->
        {
            try
            {
                semaphore.acquire();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        } );

        return semaphore;
    }

    private static void waitForStoreCopyToStartAndBlock( Semaphore storeCopyBlockingSemaphore )
    {
        assertEventually( "Read replica did not copy files", storeCopyBlockingSemaphore::hasQueuedThreads, TRUE, 60, TimeUnit.SECONDS );
    }
}
