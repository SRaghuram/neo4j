/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class RestartIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster<?> cluster;
    private ExecutorService executor;

    @BeforeAll
    void startCluster() throws ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfReadReplicas( 0 ) );
        cluster.start();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if ( executor != null )
        {
            executor.shutdownNow();
            assertTrue( executor.awaitTermination( 1, MINUTES ) );
        }
    }

    @Test
    void restartFirstServer()
    {
        // when
        cluster.removeCoreMemberWithServerId( 0 );
        cluster.addCoreMemberWithId( 0 ).start();
    }

    @Test
    void restartSecondServer()
    {
        // when
        cluster.removeCoreMemberWithServerId( 1 );
        cluster.addCoreMemberWithId( 1 ).start();
    }

    @Test
    void restartWhileDoingTransactions() throws Exception
    {
        // given
        executor = Executors.newSingleThreadExecutor();
        CountDownLatch someTransactionsCommitted = new CountDownLatch( 5 );
        AtomicBoolean done = new AtomicBoolean( false );

        // when
        Future<CoreClusterMember> transactionsFuture = executor.submit( () ->
        {
            CoreClusterMember lastWriter = null;
            while ( !done.get() )
            {
                lastWriter = cluster.coreTx( this::createNode );
                someTransactionsCommitted.countDown();
            }
            return lastWriter;
        } );

        assertTrue( someTransactionsCommitted.await( 1, MINUTES ) );

        int followerId = cluster.getMemberWithAnyRole( Role.FOLLOWER ).serverId();
        cluster.removeCoreMemberWithServerId( followerId );
        cluster.addCoreMemberWithId( followerId ).start();

        // then
        assertEventually( () -> cluster.healthyCoreMembers(), hasSize( 3 ), 1, MINUTES );
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology(), equalTo( 3 ), 1, MINUTES );

        done.set( true );

        CoreClusterMember lastWriter = transactionsFuture.get( 1, MINUTES );
        assertNotNull( lastWriter );
        dataMatchesEventually( lastWriter, cluster.coreMembers() );
    }

    @Test
    void shouldHaveWritableClusterAfterCompleteRestart() throws Exception
    {
        // given
        cluster.shutdown();

        // when
        cluster.start();

        CoreClusterMember last = cluster.coreTx( this::createNode );

        // then
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    void readReplicaTest() throws Exception
    {
        // given
        Cluster<?> cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfCoreMembers( 2 ).withNumberOfReadReplicas( 1 ) );
        cluster.start();

        // when
        CoreClusterMember last = cluster.coreTx( this::createNode );

        cluster.addCoreMemberWithId( 2 ).start();
        dataMatchesEventually( last, cluster.coreMembers() );
        dataMatchesEventually( last, cluster.readReplicas() );

        cluster.shutdown();

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            for ( CoreClusterMember core : cluster.coreMembers() )
            {
                ConsistencyCheckService.Result result =
                        new ConsistencyCheckService().runFullConsistencyCheck( DatabaseLayout.of( core.databaseDirectory() ), Config.defaults(),
                                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, false ) );
                assertTrue( result.isSuccessful(), "Inconsistent: " + core );
            }

            for ( ReadReplica readReplica : cluster.readReplicas() )
            {
                ConsistencyCheckService.Result result =
                        new ConsistencyCheckService().runFullConsistencyCheck( DatabaseLayout.of( readReplica.databaseDirectory() ), Config.defaults(),
                                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, false ) );
                assertTrue( result.isSuccessful(), "Inconsistent: " + readReplica );
            }
        }
    }

    private void createNode( GraphDatabaseService db, Transaction tx )
    {
        Node node = db.createNode( label( "boo" ) );
        node.setProperty( "foobar", "baz_bat" );
        tx.success();
    }
}
