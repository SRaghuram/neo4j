/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.configuration.FabricEnterpriseSettings;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingTableTest
{
    private static Driver clientDriver;
    private static TestFabric testFabric;
    private static DriverUtils driverUtils;
    private static TestServer testServer;

    @BeforeAll
    static void setUp()
    {
        var additionalProperties = Map.of(
                "fabric.graph.0.uri", "neo4j://somewhere:1234",
                "fabric.routing.ttl", "1234s"
        );

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withAdditionalSettings( additionalProperties )
                .build();

        testServer = testFabric.getTestServer();
        var hostPort = testServer.getHostnamePort();
        var newRoutingTable = List.of(
                socket( hostPort.getHost(), hostPort.getPort() ),
                socket( "host1", 1001 ),
                socket( "host2", 1002 ),
                socket( "host3", 1003 )
        );
        testServer.getRuntimeConfig().setDynamic( FabricEnterpriseSettings.fabric_servers_setting, newRoutingTable, "RoutingTableTest" );

        clientDriver = testFabric.routingClientDriver();

        driverUtils = new DriverUtils( "mega" );
    }

    private static SocketAddress socket( String host, int port )
    {
        return new SocketAddress( host, port );
    }

    @AfterAll
    static void tearDown()
    {
        testFabric.close();
    }

    @Test
    void testGettingRoutingTable()
    {
        List<Record> records = driverUtils.inTx( clientDriver, tx ->
        {
            var params = Map.of( "context", Map.of(), "database", "mega" );
            return tx.run( "CALL dbms.cluster.routing.getRoutingTable($context , $database)", params ).list();
        } );

        assertEquals( 1, records.size() );

        var record1 = records.get( 0 );
        assertEquals( 1234, record1.get( 0 ).asLong() );
        Value serverList = record1.get( 1 );
        var hostPort = testServer.getHostnamePort();
        // to get the required formatting of IPv6 addresses
        var serverAddress = new SocketAddress( hostPort.getHost(), hostPort.getPort() ).toString();
        verifyRole( serverList, "ROUTE", serverAddress, "host1:1001", "host2:1002", "host3:1003" );
    }

    private void verifyRole( Value serverList, String role, String... servers )
    {
        AtomicBoolean found = new AtomicBoolean( false );

        List<String> expectedServers = Arrays.asList( servers );
        Collections.sort( expectedServers );
        serverList.asList().forEach( line ->
        {
            Map<String,Object> serverType = (Map<String,Object>) line;
            String foundRole = (String) serverType.get( "role" );

            if ( foundRole.equals( role ) )
            {
                return;
            }

            List<String> foundServers = new ArrayList<>();
            ((List<String>) serverType.get( "addresses" )).forEach( s -> foundServers.add( s ) );

            Collections.sort( foundServers );

            if ( !expectedServers.equals( foundServers ) )
            {
                return;
            }

            found.set( true );
        } );

        assertTrue( found.get() );
    }
}
