/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.routing;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.procedure.builtin.routing.RoutingResult;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.impl.bag.immutable.ImmutableHashBag.newBag;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.SocketAddressParser.socketAddress;

class BaseRoutingProcedureIT
{
    private static final String OLD_ROUTING_PROCEDURE = "dbms.cluster.routing.getRoutingTable";
    private static final String NEW_ROUTING_PROCEDURE = "dbms.routing.getRoutingTable";

    static void assertPossibleToReadUsingRoutingDriver( String boltHostnamePort )
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            performRead( driver );
        }
    }

    static void assertPossibleToReadAndWriteUsingRoutingDriver( String boltHostnamePort )
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            performRead( driver );
            performWrite( driver );
        }
    }

    static void assertNotPossibleToWriteUsingRoutingDriver( String boltHostnamePort )
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                assertThrows( SessionExpiredException.class, () -> session.run( "CREATE (:Node)" ).consume() );
            }
        }
    }

    static void assertRoutingProceduresAvailable( GraphDatabaseService db, RoutingResult expectedResult )
    {
        assertAll(
                () -> assertRoutingProcedureAvailable( OLD_ROUTING_PROCEDURE, db, expectedResult ),
                () -> assertRoutingProcedureAvailable( NEW_ROUTING_PROCEDURE, db, expectedResult )
        );
    }

    private static void performRead( Driver driver )
    {
        try ( Session session = driver.session( AccessMode.READ ) )
        {
            Record record = session.readTransaction( tx -> tx.run( "RETURN 42 AS id" ).single() );
            assertEquals( 42, record.get( "id" ).asInt() );
        }
    }

    private static void performWrite( Driver driver )
    {
        try ( Session session = driver.session( AccessMode.WRITE ) )
        {
            Record record = session.writeTransaction( tx -> tx.run( "CREATE (n:Node {id: 4242}) RETURN n.id AS id" ).single() );
            assertEquals( 4242, record.get( "id" ).asInt() );
        }
    }

    private static void assertRoutingProcedureAvailable( String fullProcedureName, GraphDatabaseService db, RoutingResult expectedResult )
    {
        try ( Transaction tx = db.beginTx();
              Result result = db.execute( "CALL " + fullProcedureName + "($context)", singletonMap( "context", emptyMap() ) ) )
        {
            Map<String,Object> record = Iterators.single( result );
            RoutingResult actualResult = asRoutingResult( record );
            // compare addresses regardless of the order because procedure implementations are allowed to randomly shuffle the returned addresses
            assertEquals( newBag( expectedResult.readEndpoints() ), newBag( actualResult.readEndpoints() ), "Readers are different" );
            assertEquals( newBag( expectedResult.writeEndpoints() ), newBag( actualResult.writeEndpoints() ), "Writers are different" );
            assertEquals( newBag( expectedResult.routeEndpoints() ), newBag( actualResult.routeEndpoints() ), "Routers are different" );
            assertEquals( expectedResult.ttlMillis(), actualResult.ttlMillis() );
            tx.success();
        }
    }

    @SuppressWarnings( "unchecked" )
    private static RoutingResult asRoutingResult( Map<String,Object> record )
    {
        List<Map<String,Object>> servers = (List<Map<String,Object>>) record.get( "servers" );
        assertNotNull( servers );

        List<AdvertisedSocketAddress> readers = findAddresses( servers, Role.READ );
        List<AdvertisedSocketAddress> writers = findAddresses( servers, Role.WRITE );
        List<AdvertisedSocketAddress> routers = findAddresses( servers, Role.ROUTE );

        long ttlMillis = (long) record.get( "ttl" );

        return new RoutingResult( routers, writers, readers, ttlMillis );
    }

    @SuppressWarnings( "unchecked" )
    private static List<AdvertisedSocketAddress> findAddresses( List<Map<String,Object>> servers, Role role )
    {
        for ( Map<String,Object> entry : servers )
        {
            String roleString = (String) entry.get( "role" );
            if ( Role.valueOf( roleString ) == role )
            {
                List<String> addresses = (List<String>) entry.get( "addresses" );
                assertNotNull( addresses );

                return addresses.stream()
                        .map( address -> socketAddress( address, AdvertisedSocketAddress::new ) )
                        .collect( toList() );
            }
        }
        return emptyList();
    }

    private static Driver createDriver( String boltHostnamePort )
    {
        Config config = Config.build()
                .withLogging( Logging.none() )
                .build();

        return GraphDatabase.driver( "bolt+routing://" + boltHostnamePort, config );
    }
}
