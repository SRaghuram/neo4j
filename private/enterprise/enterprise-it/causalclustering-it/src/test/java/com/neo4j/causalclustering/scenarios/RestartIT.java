/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( PER_METHOD )
class RestartIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private ExecutorService executor;

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
    void restartFirstServer() throws Exception
    {
        // given
        cluster = startCluster( 3, 0 );

        // when
        cluster.removeCoreMemberWithServerId( 0 );
        cluster.addCoreMemberWithId( 0 ).start();
    }

    @Test
    void restartSecondServer() throws Exception
    {
        // given
        cluster = startCluster( 3, 0 );

        // when
        cluster.removeCoreMemberWithServerId( 1 );
        cluster.addCoreMemberWithId( 1 ).start();
    }

    @Test
    void restartWhileDoingTransactions() throws Exception
    {
        // given
        cluster = startCluster( 3, 0 );

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
        assertEventually( () -> cluster.healthyCoreMembers(), new HamcrestCondition<>( hasSize( 3 ) ), 1, MINUTES );
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME ), equalityCondition( 3 ), 1, MINUTES );

        done.set( true );

        CoreClusterMember lastWriter = transactionsFuture.get( 1, MINUTES );
        assertNotNull( lastWriter );
        dataMatchesEventually( lastWriter, cluster.coreMembers() );
    }

    @Test
    void shouldHaveWritableClusterAfterCompleteRestart() throws Exception
    {
        // given
        cluster = startCluster( 3, 0 );
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
        cluster = startCluster( 2, 1 );

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
                        new ConsistencyCheckService().runFullConsistencyCheck( core.databaseLayout(), Config.defaults(),
                                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, true, true, false ) );
                assertTrue( result.isSuccessful(), "Inconsistent: " + core );
            }

            for ( ReadReplica readReplica : cluster.readReplicas() )
            {
                ConsistencyCheckService.Result result =
                        new ConsistencyCheckService().runFullConsistencyCheck( readReplica.databaseLayout(), Config.defaults(),
                                ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, true, true, false ) );
                assertTrue( result.isSuccessful(), "Inconsistent: " + readReplica );
            }
        }
    }

    private void createNode( GraphDatabaseService db, Transaction tx )
    {
        Node node = tx.createNode( label( "boo" ) );
        node.setProperty( "foobar", "baz_bat" );
        tx.commit();
    }

    private Cluster startCluster( int coreCount, int readReplicaCount ) throws Exception
    {
        var clusterConfig = clusterConfig().withNumberOfCoreMembers( coreCount ).withNumberOfReadReplicas( readReplicaCount );
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }
}
