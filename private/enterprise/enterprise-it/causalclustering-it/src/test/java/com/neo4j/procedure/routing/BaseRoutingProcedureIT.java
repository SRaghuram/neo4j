/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.routing;

import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.builtin.routing.Role;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.impl.bag.immutable.ImmutableHashBag.newBag;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.helpers.SocketAddressParser.socketAddress;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DriverExtension
abstract class BaseRoutingProcedureIT
{
    @Inject
    DriverFactory driverFactory;

    private static final String CALL_NEW_PROCEDURE_WITH_CONTEXT = "CALL dbms.routing.getRoutingTable($context)";
    private static final String CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE = "CALL dbms.routing.getRoutingTable($context, $database)";

    private static final String CALL_OLD_PROCEDURE_WITH_CONTEXT = "CALL dbms.cluster.routing.getRoutingTable($context)";
    private static final String CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE = "CALL dbms.cluster.routing.getRoutingTable($context, $database)";

    void assertPossibleToReadUsingRoutingDriver( String boltHostnamePort ) throws IOException
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            performRead( driver );
        }
    }

    void assertPossibleToReadAndWriteUsingRoutingDriver( String boltHostnamePort ) throws IOException
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            performRead( driver );
            performWrite( driver );
        }
    }

    void assertNotPossibleToWriteUsingRoutingDriver( String boltHostnamePort ) throws IOException
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( WRITE ).build() ) )
            {
                assertThrows( SessionExpiredException.class, () -> session.run( "CREATE (:Node)" ).consume() );
            }
        }
    }

    void assertRoutingDriverFailsForUnknownDatabase( String boltHostnamePort, String databaseName ) throws IOException
    {
        try ( Driver driver = createDriver( boltHostnamePort ) )
        {
            var error = assertThrows( ClientException.class, () -> performRead( driver, databaseName ) );
            assertThat( error.getMessage(), containsString( "database does not exist" ) );
        }
    }

    static void assertRoutingProceduresAvailable( GraphDatabaseService db, RoutingResult expectedResult )
    {
        assertRoutingProceduresAvailable( db, expectedResult, Map.of() );
    }

    static void assertRoutingProceduresAvailable( GraphDatabaseService db, RoutingResult expectedResult, Map<String,Object> ctx )
    {
        Map<String,Object> params = paramsWithContext( ctx );

        assertAll( () -> assertRoutingProcedureAvailable( CALL_NEW_PROCEDURE_WITH_CONTEXT, params, db, expectedResult ),
                   () -> assertRoutingProcedureAvailable( CALL_OLD_PROCEDURE_WITH_CONTEXT, params, db, expectedResult ) );
    }

    static void assertRoutingProceduresAvailable( String databaseName, GraphDatabaseService db, RoutingResult expectedResult )
    {
        assertRoutingProceduresAvailable( databaseName, db, expectedResult, Map.of() );
    }

    static void assertRoutingProceduresAvailable( String databaseName, GraphDatabaseService db, RoutingResult expectedResult, Map<String,Object> ctx )
    {
        Map<String,Object> params = paramsWithContextAndDatabase( ctx, databaseName );

        assertAll( () -> assertRoutingProcedureAvailable( CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, expectedResult ),
                   () -> assertRoutingProcedureAvailable( CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, expectedResult ) );
    }

    static void assertRoutingProceduresFailForUnknownDatabase( String databaseName, GraphDatabaseService db )
    {
        assertRoutingProceduresFail( databaseName, db, DatabaseNotFound );
    }

    static void assertRoutingProceduresFailForStoppedDatabase( String databaseName, GraphDatabaseService db )
    {
        assertRoutingProceduresFail( databaseName, db, DatabaseUnavailable );
    }

    private static void assertRoutingProceduresFail( String databaseName, GraphDatabaseService db, Status failureStatus )
    {
        Map<String,Object> params = paramsWithContextAndDatabase( Map.of(), databaseName );

        assertAll( () -> assertRoutingProcedureFails( CALL_NEW_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, failureStatus ),
                () -> assertRoutingProcedureFails( CALL_OLD_PROCEDURE_WITH_CONTEXT_AND_DATABASE, params, db, failureStatus ) );
    }

    private static void performRead( Driver driver )
    {
        performRead( driver, DEFAULT_DATABASE_NAME );
    }

    private static void performRead( Driver driver, String databaseName )
    {
        try ( Session session = driver.session( builder().withDefaultAccessMode( READ ).withDatabase( databaseName ).build() ) )
        {
            Record record = session.readTransaction( tx -> tx.run( "RETURN 42 AS id" ).single() );
            assertEquals( 42, record.get( "id" ).asInt() );
        }
    }

    private static void performWrite( Driver driver )
    {
        try ( Session session = driver.session( builder().withDefaultAccessMode( WRITE ).build() ) )
        {
            Record record = session.writeTransaction( tx -> tx.run( "CREATE (n:Node {id: 4242}) RETURN n.id AS id" ).single() );
            assertEquals( 4242, record.get( "id" ).asInt() );
        }
    }

    private static void assertRoutingProcedureAvailable( String query, Map<String,Object> params, GraphDatabaseService db, RoutingResult expectedResult )
    {
        assertEventually( () -> invokeRoutingProcedure( query, params, db ), new HamcrestCondition<>( new RoutingResultMatcher( expectedResult ) ),
                2, MINUTES );
    }

    private static void assertRoutingProcedureFails( String query, Map<String,Object> params, GraphDatabaseService db, Status failureStatus )
    {
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException error = assertThrows( QueryExecutionException.class, () -> tx.execute( query, params ) );
            assertEquals( failureStatus.code().serialize(), error.getStatusCode() );
        }
    }

    private static RoutingResult invokeRoutingProcedure( String query, Map<String,Object> params, GraphDatabaseService db )
    {
        try ( var tx = db.beginTx();
              var result = tx.execute( query, params ) )
        {
            var record = Iterators.single( result );
            return asRoutingResult( record );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static RoutingResult asRoutingResult( Map<String,Object> record )
    {
        List<Map<String,Object>> servers = (List<Map<String,Object>>) record.get( "servers" );
        assertNotNull( servers );

        List<SocketAddress> readers = findAddresses( servers, Role.READ );
        List<SocketAddress> writers = findAddresses( servers, Role.WRITE );
        List<SocketAddress> routers = findAddresses( servers, Role.ROUTE );

        long ttlMillis = (long) record.get( "ttl" );

        return new RoutingResult( routers, writers, readers, ttlMillis );
    }

    @SuppressWarnings( "unchecked" )
    private static List<SocketAddress> findAddresses( List<Map<String,Object>> servers, Role role )
    {
        for ( Map<String,Object> entry : servers )
        {
            String roleString = (String) entry.get( "role" );
            if ( Role.valueOf( roleString ) == role )
            {
                List<String> addresses = (List<String>) entry.get( "addresses" );
                assertNotNull( addresses );

                return addresses.stream().map( address -> socketAddress( address, SocketAddress::new ) ).collect( toList() );
            }
        }
        return emptyList();
    }

    private Driver createDriver( String boltHostnamePort ) throws IOException
    {
        return driverFactory.graphDatabaseDriver( "neo4j://" + boltHostnamePort );
    }

    private static Map<String,Object> paramsWithContext( Map<String,Object> context )
    {
        return Map.of( "context", context );
    }

    private static Map<String,Object> paramsWithContextAndDatabase( Map<String,Object> context, String database )
    {
        return Map.of( "context", context, "database", database );
    }

    private static class RoutingResultMatcher extends TypeSafeMatcher<RoutingResult>
    {
        final RoutingResult expected;

        RoutingResultMatcher( RoutingResult expected )
        {
            this.expected = expected;
        }

        @Override
        protected boolean matchesSafely( RoutingResult actual )
        {
            // compare addresses regardless of the order because procedure implementations are allowed to randomly shuffle the returned addresses
            return newBag( expected.readEndpoints() ).equals( newBag( actual.readEndpoints() ) ) &&
                    newBag( expected.writeEndpoints() ).equals( newBag( actual.writeEndpoints() ) ) &&
                    newBag( expected.routeEndpoints() ).equals( newBag( actual.routeEndpoints() ) ) && expected.ttlMillis() == actual.ttlMillis();
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "routing result with" )
                    .appendText( " readers: " ).appendValue( expected.readEndpoints() )
                    .appendText( " writers: " ).appendValue( expected.writeEndpoints() )
                    .appendText( " routers: " ).appendValue( expected.routeEndpoints() )
                    .appendText( " ttl: " ).appendValue( expected.ttlMillis() );
        }
    }
}
