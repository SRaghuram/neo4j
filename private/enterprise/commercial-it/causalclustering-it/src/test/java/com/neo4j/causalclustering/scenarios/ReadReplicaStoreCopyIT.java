/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaStoreCopyIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( GraphDatabaseSettings.keep_logical_logs, FALSE )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    @Test( timeout = 240_000 )
    public void shouldNotBePossibleToStartTransactionsWhenReadReplicaCopiesStore() throws Throwable
    {
        Cluster<?> cluster = clusterRule.startCluster();

        ReadReplica readReplica = cluster.findAnyReadReplica();

        readReplica.txPollingClient().stop();

        writeSomeDataAndForceLogRotations( cluster );
        Semaphore storeCopyBlockingSemaphore = addStoreCopyBlockingMonitor( readReplica );
        try
        {
            readReplica.txPollingClient().start();
            waitForStoreCopyToStartAndBlock( storeCopyBlockingSemaphore );

            ReadReplicaGraphDatabase replicaGraphDatabase = readReplica.database();
            try
            {
                replicaGraphDatabase.beginTx();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( TransactionFailureException.class ) );
                assertThat( e.getMessage(), containsString( "stopped to copy a store" ) );
            }
        }
        finally
        {
            // release all waiters of the semaphore
            storeCopyBlockingSemaphore.release( Integer.MAX_VALUE );
        }
    }

    private static void writeSomeDataAndForceLogRotations( Cluster<?> cluster ) throws Exception
    {
        for ( int i = 0; i < 20; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                db.execute( "CREATE ()" );
                tx.success();
            } );

            forceLogRotationOnAllCores( cluster );
        }
    }

    private static void forceLogRotationOnAllCores( Cluster<?> cluster )
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            forceLogRotationAndPruning( core );
        }
    }

    private static void forceLogRotationAndPruning( CoreClusterMember core )
    {
        try
        {
            DependencyResolver dependencyResolver = core.database().getDependencyResolver();
            dependencyResolver.resolveDependency( LogRotation.class ).rotateLogFile();
            SimpleTriggerInfo info = new SimpleTriggerInfo( "test" );
            dependencyResolver.resolveDependency( CheckPointer.class ).forceCheckPoint( info );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
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

    private static void waitForStoreCopyToStartAndBlock( Semaphore storeCopyBlockingSemaphore ) throws Exception
    {
        assertEventually( "Read replica did not copy files", storeCopyBlockingSemaphore::hasQueuedThreads,
                is( true ), 60, TimeUnit.SECONDS );
    }
}
