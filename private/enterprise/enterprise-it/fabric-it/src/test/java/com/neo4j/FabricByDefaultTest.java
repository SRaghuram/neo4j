/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.exceptions.KernelException;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.util.FeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

class FabricByDefaultTest
{

    private static Driver driver;
    private static TestServer testServer;
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils intA = new DriverUtils( "intA" );
    private static DriverUtils intB = new DriverUtils( "intB" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll() throws KernelException
    {

        FeatureToggles.set( FabricDatabaseManager.class, FabricDatabaseManager.FABRIC_BY_DEFAULT_FLAG_NAME, true );

        var configProperties = Map.of(
                "fabric.database.name", "fabric",
                "fabric.graph.0.uri", "neo4j://dummy:1234",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();
        testServer = new TestServer( config );

        testServer.start();

        var globalProceduresRegistry = testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class );
        globalProceduresRegistry
                .registerFunction( ProxyFunctions.class );
        globalProceduresRegistry
                .registerProcedure( ProxyFunctions.class );

        driver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );

        doInTx( driver, system, tx ->
        {
            tx.run( "CREATE DATABASE intA" ).consume();
            tx.run( "CREATE DATABASE intB" ).consume();
            tx.commit();
        } );
    }

    @BeforeEach
    void beforeEach()
    {
        doInTx( driver, neo4j, tx ->
                tx.run( "MATCH (n) DETACH DELETE n" )
        );
        doInTx( driver, intA, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            tx.commit();
        } );
        doInTx( driver, intB, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Person {name: 'Carrie', uid: 100, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Dave'  , uid: 101, age: 90})" ).consume();
            tx.commit();
        } );
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> driver.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testLocalReadWrite()
    {
        doInTx( driver, neo4j, tx ->
                tx.run( "CREATE (n:User {name:'Adam'})" ).consume() );

        var result = inTx( driver, neo4j, tx ->
                tx.run( "MATCH (n:User) RETURN n.name AS name" ).list( r -> r.get( "name" ).asString() ) );

        assertThat( result ).containsExactly( "Adam" );
    }

    @Test
    void testNamedGraphs()
    {
        List<String> result =
                inTx( driver, neo4j, tx -> Stream.concat(
                        tx.run( "USE intA MATCH (n) RETURN n.name AS name" ).stream(),
                        tx.run( "USE intB MATCH (n) RETURN n.name AS name" ).stream()
                      ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() )
                );

        assertThat( result ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testReadNodes()
    {
        List<Node> r =
                inTx( driver, neo4j, tx -> Stream.concat(
                        tx.run( "USE intA MATCH (n) RETURN n" ).stream(),
                        tx.run( "USE intB MATCH (n) RETURN n" ).stream()
                      ).map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() )
                );

        var labels = r.stream().flatMap( n -> stream( n.labels() ) ).collect( Collectors.toSet() );
        assertThat( labels ).contains( "Person" );

        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testWriteNodes()
    {
        doInTx( driver, neo4j, tx ->
        {
            tx.run( "USE intA CREATE (:Cat {name: 'Whiskers'})" );
            tx.run( "USE intA CREATE (:Cat {name: 'Charlie'})" );
        } );

        List<Node> r = inTx( driver, neo4j, tx ->
        {
            tx.run( "USE intB CREATE (:Cat {name: 'Misty'})" );
            tx.run( "USE intB CREATE (:Cat {name: 'Cupcake'})" );
            return Stream.concat(
                    tx.run( "USE intA MATCH (c:Cat) RETURN c" ).stream(),
                    tx.run( "USE intB MATCH (c:Cat) RETURN c" ).stream()
            ).map( c -> c.get( "c" ).asNode() ).collect( Collectors.toList() );
        } );

        var labels = r.stream().flatMap( n -> stream( n.labels() ) ).collect( Collectors.toSet() );
        assertThat( labels ).contains( "Cat" );
        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names ).contains( "Whiskers", "Charlie", "Misty", "Cupcake" );
    }

    @Test
    void testOptionalValue()
    {
        doInTx( driver, neo4j, tx ->
                tx.run( "USE intA CREATE (:User {id:1})-[:FRIEND]->(:User)" )
        );
        doInTx( driver, neo4j, tx ->
                tx.run( "USE intA MATCH (n:User{id:1})-[:FRIEND]->(x:User) OPTIONAL MATCH (x)-[:FRIEND]->(y:User) RETURN x, y" )
                  .consume()
        );
    }

    @Test
    void testLocalSingleReturn()
    {
        List<Record> r = inTx( driver, neo4j, tx -> tx.run( "RETURN 1+2 AS a, 'foo' AS f" ).list() );

        assertThat( r.get( 0 ).get( "a" ).asInt() ).isEqualTo( 3 );
        assertThat( r.get( 0 ).get( "f" ).asString() ).isEqualTo( "foo" );
    }

    @Test
    void testSubqueryWithCreate()
    {
        List<Record> r = inTx( driver, neo4j, tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  CREATE (y:Foo {p: 123})",
                    "  RETURN y",
                    "}",
                    "RETURN x, y"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).get( "x" ).asLong() ).isEqualTo( 1L );
        assertThat( r.get( 0 ).get( "y" ).asNode().labels() ).containsExactly( "Foo" );
        assertThat( r.get( 0 ).get( "y" ).asNode().get( "p" ).asLong() ).isEqualTo( 123L );
    }

    @Test
    void testSubqueryWithSet()
    {
        List<Record> r = inTx( driver, intA, tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  MATCH (y:Person {age: 30})",
                    "  SET y.age = 100",
                    "  RETURN y",
                    "}",
                    "SET y.age = 200",
                    "RETURN y"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).get( "y" ).asNode().get( "age" ).asLong() ).isEqualTo( 200L );
    }

    @Test
    void testWriteInReadModeShouldFail()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( driver, neo4j, AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  CREATE (n:Test)",
                    "  RETURN n",
                    "}",
                    "RETURN n"
            );
            tx.run( query ).list();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "Writing in read access mode not allowed" );
    }

    @Test
    void testCallReadProcedure()
    {
        List<String> result = inTx( driver, neo4j, AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "CALL com.neo4j.utils.reader() YIELD foo",
                    "RETURN foo"
            );
            return tx.run( query ).stream().map( r -> r.get( "foo" ).asString() ).collect( Collectors.toList() );
        } );

        assertThat( result.size() ).isEqualTo( 1 );
        assertThat( result ).contains( "read" );
    }

    @Test
    void testCallWriteProcedureInRead()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( driver, neo4j, AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "CALL com.neo4j.utils.writer() YIELD foo",
                    "RETURN foo"
            );
            tx.run( query ).consume();
        } ) );

        assertThat( ex.getMessage() ).contains( "Writing in read access mode not allowed" );
    }

    private <T> T inTx( Driver driver, DriverUtils driverUtils, AccessMode accessMode, Function<Transaction,T> workload )
    {
        return driverUtils.inTx( driver, accessMode, workload );
    }

    private void doInTx( Driver driver, DriverUtils driverUtils, AccessMode accessMode, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( driver, accessMode, workload );
    }

    private static <T> T inTx( Driver driver, DriverUtils driverUtils, Function<Transaction,T> workload )
    {
        return driverUtils.inTx( driver, workload );
    }

    private static void doInTx( Driver driver, DriverUtils driverUtils, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( driver, workload );
    }
}
