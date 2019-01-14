/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

/**
 * Test various IPv6 configuration options on a single HA instance.
 */
public class HaIPv6ConfigurationIT
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void testClusterWithLocalhostAddresses()
    {
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.storeDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::1", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void testClusterWithLinkLocalAddress() throws Throwable
    {
        InetAddress inetAddress;

        Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
        while ( nics.hasMoreElements() )
        {
            NetworkInterface nic = nics.nextElement();
            Enumeration<InetAddress> inetAddresses = nic.getInetAddresses();
            while ( inetAddresses.hasMoreElements() )
            {
                inetAddress = inetAddresses.nextElement();
                if ( inetAddress instanceof Inet6Address && inetAddress.isLinkLocalAddress() )
                {
                    try
                    {
                        if ( inetAddress.isReachable( 1000 ) )
                        {
                            testWithAddress( inetAddress );
                        }
                    }
                    catch ( ConnectException e )
                    {
                        // fine, just ignore
                    }
                }
            }
        }
    }

    private void testWithAddress( InetAddress inetAddress )
    {
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.storeDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( inetAddress.getHostAddress(), clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( inetAddress.getHostAddress(), clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    @Test
    public void testClusterWithWildcardAddresses()
    {
        int clusterPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.storeDir() )
                .setConfig( ClusterSettings.cluster_server, ipv6HostPortSetting( "::", clusterPort ) )
                .setConfig( ClusterSettings.initial_hosts, ipv6HostPortSetting( "::1", clusterPort ) )
                .setConfig( HaSettings.ha_server, ipv6HostPortSetting( "::", PortAuthority.allocatePort() ) )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();
    }

    private static String ipv6HostPortSetting( String address, int port )
    {
        return "[" + address + "]:" + port;
    }
}
