/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
class RestartIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster<?> cluster;

    @BeforeAll
    void startCluster() throws ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfReadReplicas( 0 ) );
        cluster.start();
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
        // when
        final GraphDatabaseService coreDB = cluster.getCoreMemberById( 0 ).database();

        ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean done = new AtomicBoolean( false );
        executor.execute( () ->
        {
            while ( !done.get() )
            {
                try ( Transaction tx = coreDB.beginTx() )
                {
                    Node node = coreDB.createNode( label( "boo" ) );
                    node.setProperty( "foobar", "baz_bat" );
                    tx.success();
                }
                catch ( AcquireLockTimeoutException | WriteOperationsNotAllowedException e )
                {
                    // expected sometimes
                }
            }
        } );
        Thread.sleep( 500 );

        cluster.removeCoreMemberWithServerId( 1 );
        cluster.addCoreMemberWithId( 1 ).start();
        Thread.sleep( 500 );

        // then
        done.set( true );
        executor.shutdown();
    }

    @Test
    void shouldHaveWritableClusterAfterCompleteRestart() throws Exception
    {
        // given
        cluster.shutdown();

        // when
        cluster.start();

        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

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
        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

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
}
