/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( FabricEverywhereExtension.class )
class CommunityEditionEndToEndTest
{

    private static Neo4j server;
    private static Driver driver;

    @BeforeAll
    static void beforeAll()
    {
        server = Neo4jBuilders.newInProcessBuilder().build();

        driver = GraphDatabase.driver(
                server.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            tx.commit();
        }
    }

    @AfterAll
    static void afterAll()
    {
        server.close();
        driver.close();
    }

    @Test
    void testUse()
    {
        doTestUse( DEFAULT_DATABASE_NAME );
        doTestUse( SYSTEM_DATABASE_NAME );
    }

    @Test
    void testSystemCommandRouting()
    {
        execute( DEFAULT_DATABASE_NAME, session -> session.run( "CREATE USER foo SET PASSWORD 'bar'" ).consume() );

        List<String> result = execute( DEFAULT_DATABASE_NAME, session -> session.run( "SHOW USERS" ).list() )
                .stream()
                .map( r -> r.get( "user" ).asString() ).collect( Collectors.toList() );

        assertThat( result ).contains( "foo" );
    }

    @Test
    void testSchemaCommand()
    {
        doTestSchemaCommands( DEFAULT_DATABASE_NAME );
        doTestSchemaCommands( SYSTEM_DATABASE_NAME );
    }

    private void doTestUse( String database )
    {
        List<String> result = execute( database, session -> session.run( "USE " + DEFAULT_DATABASE_NAME + " MATCH (n) RETURN n.name AS name" ).list() )
                .stream()
                .map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );

        assertThat( result ).contains( "Anna", "Bob" );
    }

    private void doTestSchemaCommands( String database )
    {
        execute( database, session -> session.run( "USE " + DEFAULT_DATABASE_NAME + " CREATE INDEX myIndex FOR (n:Person) ON (n.name)" ).consume() );

        List<String> result = execute( database, session -> session.run( "USE " + DEFAULT_DATABASE_NAME + " CALL db.indexes() YIELD name RETURN name" ).list() )
                .stream()
                .map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( result ).contains( "myIndex" );

        execute( database, session -> session.run( "USE " + DEFAULT_DATABASE_NAME + " DROP INDEX myIndex" ).consume() );
    }

    private <T> T execute( String database, Function<Session,T> workload )
    {
        try ( var session = driver.session( SessionConfig.forDatabase( database ) ) )
        {
            return workload.apply( session );
        }
    }
}
