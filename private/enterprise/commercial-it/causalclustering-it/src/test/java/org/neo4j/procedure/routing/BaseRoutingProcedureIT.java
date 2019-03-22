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
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.procedure.builtin.routing.RoutingResult;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.impl.bag.immutable.ImmutableHashBag.newBag;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.SocketAddressParser.socketAddress;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;

class BaseRoutingProcedureIT
{
    private static final String CALL_NEW_PROCEDURE_WITH_CONTEXT = "CALL dbms.routing.getRoutingTable($context)";
    private static final String CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE = "CALL dbms.routing.getRoutingTable($context, $database)";

    private static final String CALL_OLD_PROCEDURE_WITH_CONTEXT = "CALL dbms.cluster.routing.getRoutingTable($context)";
    private static final String CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE = "CALL dbms.cluster.routing.getRoutingTable($context, $database)";

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
        Map<String,Object> params = paramsWithContext( Map.of() );

        assertAll(
                () -> assertRoutingProcedureAvailable( CALL_NEW_PROCEDURE_WITH_CONTEXT, params, db, expectedResult ),
                () -> assertRoutingProcedureAvailable( CALL_OLD_PROCEDURE_WITH_CONTEXT, params, db, expectedResult )
        );
    }

    static void assertRoutingProceduresAvailable( String databaseName, GraphDatabaseService db, RoutingResult expectedResult )
    {
        Map<String,Object> params = paramsWithContextAndDatabase( Map.of(), databaseName );

        assertAll(
                () -> assertRoutingProcedureAvailable( CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, expectedResult ),
                () -> assertRoutingProcedureAvailable( CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, expectedResult )
        );
    }

    static void assertRoutingProceduresFailForUnknownDatabase( String databaseName, GraphDatabaseService db )
    {
        Map<String,Object> params = paramsWithContextAndDatabase( Map.of(), databaseName );

        assertAll(
                () -> assertRoutingProcedureFailsForUnknownDatabase( CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db ),
                () -> assertRoutingProcedureFailsForUnknownDatabase( CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db )
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

    private static void assertRoutingProcedureAvailable( String query, Map<String,Object> params, GraphDatabaseService db, RoutingResult expectedResult )
    {
        try ( Transaction tx = db.beginTx();
              Result result = db.execute( query, params ) )
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

    private static void assertRoutingProcedureFailsForUnknownDatabase( String query, Map<String,Object> params, GraphDatabaseService db )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> db.execute( query, params ) );
            assertEquals( DatabaseNotFound.code().serialize(), error.getStatusCode() );
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

    private static Map<String,Object> paramsWithContext( Map<String,Object> context )
    {
        return Map.of( "context", context );
    }

    private static Map<String,Object> paramsWithContextAndDatabase( Map<String,Object> context, String database )
    {
        return Map.of( "context", context, "database", database );
    }
}
