/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package slavetest;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/*
 * This test case ensures that instances with the same store id but very old txids
 * will successfully join with a full version of the store.
 */
public class InstanceJoinIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void makeSureSlaveCanJoinEvenIfTooFarBackComparedToMaster() throws Exception
    {
        String key = "foo";
        String value = "bar";

        HighlyAvailableGraphDatabase master = null;
        HighlyAvailableGraphDatabase slave = null;
        File masterDir = testDirectory.databaseDir( "master" );
        File slaveDir = testDirectory.databaseDir( "slave" );
        try
        {
            int masterClusterPort = PortAuthority.allocatePort();
            int masterHaPort = PortAuthority.allocatePort();
            master = start( masterDir, 0,
                    stringMap( keep_logical_logs.name(), "1 txs",
                               ClusterSettings.initial_hosts.name(), "127.0.0.1:" + masterClusterPort ), masterClusterPort, masterHaPort );
            createNode( master, "something", "unimportant" );
            checkPoint( master );
            // Need to start and shutdown the slave so when we start it up later it verifies instead of copying
            int slaveClusterPort = PortAuthority.allocatePort();
            int slaveHaPort = PortAuthority.allocatePort();
            slave = start( slaveDir, 1,
                    stringMap( ClusterSettings.initial_hosts.name(), "127.0.0.1:" + masterClusterPort ), slaveClusterPort, slaveHaPort );
            slave.shutdown();

            createNode( master, key, value );
            checkPoint( master );
            // Rotating, moving the above transactions away so they are removed on shutdown.
            rotateLog( master );

            /*
             * We need to shutdown - rotating is not enough. The problem is that log positions are cached and they
             * are not removed from the cache until we run into the cache limit. This means that the information
             * contained in the log can actually be available even if the log is removed. So, to trigger the case
             * of the master information missing from the master we need to also flush the log entry cache - hence,
             * restart.
             */
            master.shutdown();
            master = start( masterDir, 0,
                    stringMap( keep_logical_logs.name(), "1 txs",
                               ClusterSettings.initial_hosts.name(), "127.0.0.1:" + masterClusterPort ), masterClusterPort, masterHaPort );

            /*
             * The new log on master needs to have at least one transaction, so here we go.
             */
            int importantNodeCount = 10;
            for ( int i = 0; i < importantNodeCount; i++ )
            {
                createNode( master, key, value );
                checkPoint( master );
                rotateLog( master );
            }

            checkPoint( master );

            slave = start( slaveDir, 1,
                    stringMap( ClusterSettings.initial_hosts.name(), "127.0.0.1:" + masterClusterPort ), slaveClusterPort, slaveHaPort );
            slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();

            try ( Transaction ignore = slave.beginTx() )
            {
                assertEquals( "store contents differ", importantNodeCount + 1,
                        nodesHavingProperty( slave, key, value ) );
            }
        }
        finally
        {
            if ( slave != null )
            {
                slave.shutdown();
            }

            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    private static void rotateLog( HighlyAvailableGraphDatabase db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
    }

    private static void checkPoint( HighlyAvailableGraphDatabase db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private static int nodesHavingProperty( HighlyAvailableGraphDatabase slave, String key, String value )
    {
        try ( Transaction tx = slave.beginTx() )
        {
            int count = 0;
            for ( Node node : slave.getAllNodes() )
            {
                if ( value.equals( node.getProperty( key, null ) ) )
                {
                    count++;
                }
            }
            tx.success();
            return count;
        }
    }

    private static void createNode( HighlyAvailableGraphDatabase db, String key, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            tx.success();
        }
    }

    private static HighlyAvailableGraphDatabase start( File storeDir, int i, Map<String, String> additionalConfig, int clusterPort, int haPort )
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort )
                .setConfig( ClusterSettings.server_id, i + "" )
                .setConfig( HaSettings.ha_server, "127.0.0.1:" + haPort )
                .setConfig( HaSettings.pull_interval, "0ms" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .setConfig( additionalConfig )
                .newGraphDatabase();

        awaitStart( db );
        return db;
    }

    private static void awaitStart( HighlyAvailableGraphDatabase db )
    {
        db.beginTx().close();
    }
}
