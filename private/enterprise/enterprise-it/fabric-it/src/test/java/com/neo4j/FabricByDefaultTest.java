/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.types.Node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

class FabricByDefaultTest
{

    private static Driver driver;
    private static TestFabric testFabric;
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils intA = new DriverUtils( "intA" );
    private static DriverUtils intB = new DriverUtils( "intB" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "fabric" )
                .registerFuncOrProc( ProxyFunctions.class )
                .build();

        driver = testFabric.routingClientDriver();

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
        testFabric.close();
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

    @Test
    void testPeriodicCommitOnLocalGraph() throws IOException
    {
        Path csvFile = Files.createTempFile("fabric-test", "");
        try
        {
            var csvContent = joinAsLines(
                    "Eva",
                    "Fiona",
                    "Gustav"
            );

            Files.writeString( csvFile, csvContent );

            var query = joinAsLines(
                    "USING PERIODIC COMMIT 1",
                    "LOAD CSV FROM $csv AS row",
                    "WITH row[0] AS name",
                    "CREATE (:Person {name:name})"
            );

            try ( var session = driver.session() )
            {
                session.run( query, Map.of( "csv", csvFile.toAbsolutePath().toUri().toString() ) ).consume();
            }

            var matchQuery = joinAsLines(
                    "MATCH (n:Person)",
                    "RETURN n.name AS name"
            );

            var records = inTx( driver, neo4j, tx -> tx.run( matchQuery ).list() );
            List<String> names = records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );

            assertThat( names ).containsExactlyInAnyOrder( "Eva", "Fiona", "Gustav" );
        }
        finally
        {
            Files.delete( csvFile );
        }
    }

    @Test
    void testIncorrectBookmark()
    {
        Bookmark receivedBookmark;
        try ( var session = driver.session() )
        {
            session.run( "RETURN 1" ).list();
            receivedBookmark = session.lastBookmark();
        }

        Bookmark modifiedBookmark = Bookmark.from( receivedBookmark.values().stream().map( v -> v + "O" ).collect( Collectors.toSet() ) );

        try ( var session = driver.session( SessionConfig.builder().withBookmarks( modifiedBookmark ).build() ) )
        {
            assertThatThrownBy( () -> session.run( "RETURN 1" ).list() )
                    .isInstanceOf( ClientException.class )
                    .hasMessageContaining( "Parsing of supplied bookmarks failed with message:" );
        }
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
