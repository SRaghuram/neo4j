/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.exceptions.KernelException;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingTableTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static PortUtils.Ports ports;

    @BeforeAll
    static void setUp() throws KernelException
    {
        ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "somewhere:1234",
                "fabric.routing.ttl", "1234000",
                "fabric.routing.servers", "localhost:" + ports.bolt + ",host1:1001,host2:1002,host3:1003",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( CustomFunctions.class );

        clientDriver = GraphDatabase.driver(
                "neo4j://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.stop();
        clientDriver.close();
    }

    @Test
    void testGettingRoutingTable()
    {

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var params = Map.of( "context", Map.of(), "database", "mega" );
            var records = tx.run( "CALL dbms.cluster.routing.getRoutingTable($context , $database)", params ).list();
            assertEquals( 1, records.size() );

            var record1 = records.get( 0 );
            assertEquals( 1234, record1.get( 0 ).asLong() );
            Value serverList = record1.get( 1 );
            verifyRole( serverList, "ROUTE", "localhost:" + ports.bolt, "host1:1001", "host2:1002", "host3:1003" );

            tx.success();
        }
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
