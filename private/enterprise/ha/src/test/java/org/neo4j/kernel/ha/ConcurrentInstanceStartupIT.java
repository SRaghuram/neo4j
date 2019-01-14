/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class ConcurrentInstanceStartupIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void concurrentStartupShouldWork() throws Exception
    {
        // Ensures that the instances don't race to create the test's base directory and only care about their own.
        testDirectory.directory( "nothingToSeeHereMoveAlong" );
        int[] clusterPorts = new int[]{
                PortAuthority.allocatePort(),
                PortAuthority.allocatePort(),
                PortAuthority.allocatePort()
        };
        final String initialHosts = initialHosts( clusterPorts );
        final CyclicBarrier barrier = new CyclicBarrier( clusterPorts.length );
        final List<Thread> daThreads = new ArrayList<>( clusterPorts.length );
        final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[clusterPorts.length];

        for ( int i = 0; i < clusterPorts.length; i++ )
        {
            final int finalI = i;

            Thread t = new Thread( () ->
            {
                try
                {
                    barrier.await();
                    dbs[finalI] = startDbAtBase( finalI, initialHosts, clusterPorts[finalI] );
                }
                catch ( InterruptedException | BrokenBarrierException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            daThreads.add( t );
            t.start();
        }

        for ( Thread daThread : daThreads )
        {
            daThread.join();
        }

        boolean masterDone = false;

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            if ( db.isMaster() )
            {
                if ( masterDone )
                {
                    throw new Exception( "Two masters discovered" );
                }
                masterDone = true;
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.success();
            }
        }

        assertTrue( masterDone );

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            db.shutdown();
        }
    }

    private String initialHosts( int[] clusterPorts )
    {
        return IntStream.of( clusterPorts )
                .mapToObj( i -> "127.0.0.1:" + i )
                .collect( Collectors.joining( "," ) );
    }

    private HighlyAvailableGraphDatabase startDbAtBase( int i, String initialHosts, int clusterPort )
    {
        GraphDatabaseBuilder masterBuilder = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path( i ).getAbsoluteFile() )
                .setConfig( ClusterSettings.initial_hosts, initialHosts )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort )
                .setConfig( ClusterSettings.server_id, "" + i )
                .setConfig( HaSettings.ha_server, ":" + PortAuthority.allocatePort() )
                .setConfig( HaSettings.tx_push_factor, "0" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return (HighlyAvailableGraphDatabase) masterBuilder.newGraphDatabase();
    }

    private File path( int i )
    {
        return testDirectory.databaseDir( i + "" );
    }
}
