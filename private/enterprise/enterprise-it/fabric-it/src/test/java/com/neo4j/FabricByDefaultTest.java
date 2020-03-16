/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.bolt.FabricBookmarkParser;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import com.neo4j.utils.ShardFunctions;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.util.FeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

class FabricByDefaultTest
{

    private static Driver clientDriver;
    private static TestServer testServer;
    private static Neo4j shard0;
    private static Neo4j shard1;
    private static Driver shard0Driver;
    private static Driver shard1Driver;
    private static DriverUtils driverUtils;

    @BeforeAll
    static void beforeAll() throws KernelException
    {

        FeatureToggles.set( FabricDatabaseManager.class, FabricDatabaseManager.FABRIC_BY_DEFAULT_FLAG_NAME, true );

        shard0 = Neo4jBuilders.newInProcessBuilder().withProcedure( ShardFunctions.class ).build();
        shard1 = Neo4jBuilders.newInProcessBuilder().withProcedure( ShardFunctions.class ).build();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.graph.0.name", "myGraph0",
                "fabric.graph.1.uri", shard1.boltURI().toString(),
                "fabric.graph.1.name", "myGraph1",
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

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        shard0Driver = GraphDatabase.driver(
                shard0.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );
        shard1Driver = GraphDatabase.driver(
                shard1.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        driverUtils = new DriverUtils( "neo4j" );
    }

    @BeforeEach
    void beforeEach()
    {
        doInTx( tx -> tx.run( "MATCH (n) DETACH DELETE n" ) );

        try ( Transaction tx = shard0Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            tx.commit();
        }
        try ( Transaction tx = shard1Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Person {name: 'Carrie', uid: 100, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Dave'  , uid: 101, age: 90})" ).consume();
            tx.commit();
        }
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> shard0Driver.close(),
                () -> shard1Driver.close(),
                () -> shard0.close(),
                () -> shard1.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testLocalReadWrite()
    {
        inTx( tx -> tx.run( "CREATE (n:User {name:'Adam'})" ).consume() );

        var result = inTx( tx -> tx.run( "MATCH (n:User) RETURN n.name AS name" ).list( r -> r.get( "name" ).asString() ) );

        assertThat( result ).containsExactly( "Adam" );
    }

    @Test
    void testMixedReadWrite()
    {
        inTx( tx -> tx.run( "CREATE (n:User {name:'Adam'})" ).consume() );

        var result =
                inTx( tx -> tx.run( joinAsLines(
                        "MATCH (loc:User)",
                        "CALL {",
                        "  USE mega.myGraph0",
                        "  MATCH (rem:Person) RETURN rem",
                        "}",
                        "RETURN loc.name + rem.name AS compositeName" )
                ).list( r -> r.get( "compositeName" ).asString() ) );

        assertThat( result ).containsExactlyInAnyOrder( "AdamAnna", "AdamBob" );
    }

    @Test
    void testMixedReadWriteSet()
    {
        inTx( tx -> tx.run( "CREATE (n:User {name:'Adam'})" ).consume() );

        var result =
                inTx( tx -> tx.run( joinAsLines(
                        "MATCH (user:User)",
                        "SET user.name = 'Betty'",
                        "WITH user",
                        "CALL {",
                        "  USE mega.myGraph0",
                        "  MATCH (person:Person) RETURN person",
                        "}",
                        "RETURN user.name + person.name AS compositeName" )
                ).list( r -> r.get( "compositeName" ).asString() ) );

        assertThat( result ).containsExactlyInAnyOrder( "BettyAnna", "BettyBob" );
    }

    @Test
    void testReadStrings()
    {
        List<String> result = inTx( tx ->
                Stream.concat(
                        tx.run( "USE mega.graph(0) MATCH (n) RETURN n.name AS name" ).stream(),
                        tx.run( "USE mega.graph(1) MATCH (n) RETURN n.name AS name" ).stream()
                ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() )
        );

        assertThat( result ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testNamedGraphs()
    {
        List<String> result = inTx( tx ->
                Stream.concat(
                        tx.run( "USE mega.myGraph0 MATCH (n) RETURN n.name AS name" ).stream(),
                        tx.run( "USE mega.myGraph1 MATCH (n) RETURN n.name AS name" ).stream()
                ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() )
        );

        assertThat( result ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testReadStringsFromView()
    {
        List<String> result = inTx( tx ->
        {
            Map<String,Object> sid0 = Map.of( "sid", 0 );
            Map<String,Object> sid1 = Map.of( "sid", 1 );

            return Stream.concat(
                    tx.run( "USE mega.graph($sid) MATCH (n) RETURN n.name AS name", sid0 ).stream(),
                    tx.run( "USE mega.graph($sid) MATCH (n) RETURN n.name AS name", sid1 ).stream()
            ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
        } );

        assertThat( result ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testReadNodes()
    {
        List<Node> r = inTx( tx ->
                Stream.concat(
                        tx.run( "USE mega.graph(0) MATCH (n) RETURN n" ).stream(),
                        tx.run( "USE mega.graph(1) MATCH (n) RETURN n" ).stream()
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
        doInTx( tx ->
        {
            tx.run( "USE mega.graph(0) CREATE (:Cat {name: 'Whiskers'})" );
            tx.run( "USE mega.graph(0) CREATE (:Cat {name: 'Charlie'})" );
        } );

        List<Node> r = inTx( tx ->
        {
            tx.run( "USE mega.graph(1) CREATE (:Cat {name: 'Misty'})" );
            tx.run( "USE mega.graph(1) CREATE (:Cat {name: 'Cupcake'})" );
            return Stream.concat(
                    tx.run( "USE mega.graph(0) MATCH (c:Cat) RETURN c" ).stream(),
                    tx.run( "USE mega.graph(1) MATCH (c:Cat) RETURN c" ).stream()
            ).map( c -> c.get( "c" ).asNode() ).collect( Collectors.toList() );
        } );

        var labels = r.stream().flatMap( n -> stream( n.labels() ) ).collect( Collectors.toSet() );
        assertThat( labels ).contains( "Cat" );
        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names ).contains( "Whiskers", "Charlie", "Misty", "Cupcake" );
    }

    @Test
    void testCustomShardKeyMapping()
    {
        List<Node> r = inTx( tx ->
        {
            Map<String,Object> uid = Map.of( "uid", 100 );
            return tx.run( joinAsLines(
                    "USE mega.graph(com.neo4j.utils.personShard($uid))",
                    "MATCH (n {uid: $uid})",
                    "RETURN n"
            ), uid ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).labels() ).contains( "Person" );
        assertThat( r.get( 0 ).get( "name" ).asString() ).isEqualTo( "Carrie" );
        assertThat( r.get( 0 ).get( "uid" ).asInt() ).isEqualTo( 100 );
    }

    @Test
    void testReadUnionAll()
    {
        List<Node> r = inTx( tx ->
                tx.run( joinAsLines(
                        "USE mega.graph(0) MATCH (n) RETURN n",
                        "UNION ALL",
                        "USE mega.graph(1) MATCH (n) RETURN n"
                ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() )
        );

        assertThat( r.size() ).isEqualTo( 4 );
        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        labels.forEach( l -> assertThat( l ).contains( "Person" ) );

        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testReadUnionDistinct()
    {
        List<Node> r = inTx( tx ->
                tx.run( joinAsLines(
                        "USE mega.graph(0) MATCH (n) RETURN n",
                        "UNION",
                        "USE mega.graph(1) MATCH (n) RETURN n"
                ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() )
        );

        assertThat( r.size() ).isEqualTo( 4 );
        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        labels.forEach( l -> assertThat( l ).contains( "Person" ) );
        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names ).contains( "Anna", "Bob", "Carrie", "Dave" );
    }

    @Test
    void testReadUnionAllValues()
    {
        List<Integer> r = inTx( tx ->
                tx.run( joinAsLines(
                        "USE mega.graph(0) MATCH (n) RETURN n.age AS a",
                        "UNION ALL",
                        "USE mega.graph(1) MATCH (n) RETURN n.age AS a"
                ) ).stream().map( c -> c.get( "a" ).asInt() ).collect( Collectors.toList() )
        );

        assertThat( r ).contains( 30, 40, 90 );
    }

    @Test
    void testReadUnionDistinctValues()
    {
        List<Integer> r = inTx( tx ->
                tx.run( joinAsLines(
                        "USE mega.graph(0) MATCH (n) RETURN n.age AS a",
                        "UNION",
                        "USE mega.graph(1) MATCH (n) RETURN n.age AS a"
                ) ).stream().map( c -> c.get( "a" ).asInt() ).collect( Collectors.toList() )
        );

        assertThat( r ).contains( 30, 40, 90 );
    }

    @Test
    void testOptionalValue()
    {
        doInTx( tx -> tx.run( "USE mega.graph(0) CREATE (:User {id:1}) - [:FRIEND] -> (:User)" ) );
        doInTx( tx ->
                tx.run( "USE mega.graph(0) MATCH (n:User{id:1})-[:FRIEND]->(x:User) OPTIONAL MATCH (x)-[:FRIEND]->(y:User) RETURN x, y" )
                        .consume()
        );
    }

    @Test
    void testLocalSingleReturn()
    {
        List<Record> r = inTx( tx -> tx.run( "RETURN 1+2 AS a, 'foo' AS f" ).list() );

        assertThat( r.get( 0 ).get( "a" ).asInt() ).isEqualTo( 3 );
        assertThat( r.get( 0 ).get( "f" ).asString() ).isEqualTo( "foo" );
    }

    @Test
    void testReadFromShardWithProxyAliasing()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN x AS Sid, y AS Person" );
            return tx.run( query ).list();
        } );

        assertEquals( 4, r.size() );
        var personToSid = r.stream().collect( Collectors.toMap( e -> e.get( "Person" ).asNode().get( "name" ).asString(), e -> e.get( "Sid" ).asInt() ) );
        assertEquals( Map.of( "Anna", 0, "Bob", 0, "Carrie", 1, "Dave", 1 ), personToSid );
    }

    @Test
    void testAllGraphsRead()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND mega.graphIds() AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN gid, y AS person",
                    "  ORDER BY person.uid" );
            return tx.run( query ).list();
        } );

        var graphIds = r.stream()
                .map( c -> c.get( "gid" ).asInt() )
                .collect( Collectors.toList() );

        var persons = r.stream()
                .map( c -> c.get( "person" ).asNode() )
                .collect( Collectors.toList() );

        assertThat( graphIds ).isEqualTo( List.of( 0, 0, 1, 1 ) );

        assertThat( r.size() ).isEqualTo( 4 );
        verifyPerson( persons, 0, "Anna" );
        verifyPerson( persons, 1, "Bob" );
        verifyPerson( persons, 2, "Carrie" );
        verifyPerson( persons, 3, "Dave" );
    }

    private void verifyPerson( List<Node> r, int index, String name )
    {
        assertThat( r.get( index ).labels() ).contains( "Person" );
        assertThat( r.get( index ).get( "name" ).asString() ).isEqualTo( name );
    }

    @Test
    void testIdTagging()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND mega.graphIds() AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  MATCH (p)",
                    "  RETURN p, id(p) AS local_id",
                    "}",
                    "RETURN gid, local_id, id(p) as tagged_id" );
            return tx.run( query ).list();
        } );

        var gids = r.stream().map( c -> c.get( "gid" ).asLong() ).distinct().count();
        var local = r.stream().map( c -> c.get( "local_id" ).asLong() ).distinct().count();
        var tagged = r.stream().map( c -> c.get( "tagged_id" ).asLong() ).distinct().count();

        assertThat( gids ).isEqualTo( 2L );
        assertThat( local ).isGreaterThanOrEqualTo( 2L ).isLessThanOrEqualTo( 4L );
        assertThat( tagged ).isEqualTo( 4L );
    }

    @Test
    void testReadFromShardWithProxyOrdering()
    {
        List<String> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN y.name AS name",
                    "ORDER BY name DESC" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Dave", "Carrie", "Bob", "Anna" ) );
    }

    @Test
    void testReadFromShardWithProxyAggregation()
    {
        List<Record> records = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y.age AS age, y.name AS name",
                    "}",
                    "RETURN age, collect(name) AS names",
                    "ORDER BY age DESC" );

            return tx.run( query ).list();
        } );

        assertThat( records.size() ).isEqualTo( 3 );
        assertThat( records.get( 0 ).keys() ).containsExactly( "age", "names" );
        assertThat( records.get( 0 ).get( 0 ).asInt() ).isEqualTo( 90 );
        assertThat( records.get( 0 ).get( 1 ).asList() ).contains( "Dave" );
        assertThat( records.get( 1 ).keys() ).containsExactly( "age", "names" );
        assertThat( records.get( 1 ).get( 0 ).asInt() ).isEqualTo( 40 );
        assertThat( records.get( 1 ).get( 1 ).asList() ).contains( "Bob" );
        assertThat( records.get( 2 ).keys() ).containsExactly( "age", "names" );
        assertThat( records.get( 2 ).get( 0 ).asInt() ).isEqualTo( 30 );
        assertThat( records.get( 2 ).get( 1 ).asList() ).contains( "Anna", "Carrie" );
    }

    @Test
    void testColumnJuggling()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  RETURN 2 AS y",
                    "}",
                    "WITH 3 AS z, y AS y",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH y",
                    "  CREATE (a: A)",
                    "  RETURN y*10 AS x",
                    "}",
                    "CALL {",
                    "  WITH 0 AS a",
                    "  RETURN 4 AS w",
                    "}",
                    "RETURN z, w, y, x"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "z", "w", "y", "x" );
        assertThat( r.get( 0 ).values() ).containsExactly( Values.value( 3 ), Values.value( 4 ), Values.value( 2 ), Values.value( 20 ) );
    }

    @Test
    @Disabled
    void testDisallowRemoteSubqueryInRemoteSubquery()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [1, 2, 3] AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH x",
                    "  WITH x*10 AS y",
                    "  CALL {",
                    "    WITH y",
                    "    WITH y*10 AS z",
                    "    RETURN z",
                    "  }",
                    "  RETURN y, z, z*10 AS w",
                    "}",
                    "RETURN x, y, z, w"
            );

            tx.run( query ).consume();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "Nested subqueries in remote query-parts is not supported" );
    }

    @Test
    void testSubqueryWithCreate()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
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
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (y:Person {age: 30})",
                    "  SET y.age = 100",
                    "  RETURN y",
                    "}",
                    "RETURN y"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).get( "y" ).asNode().get( "age" ).asLong() ).isEqualTo( 100L );
    }

    @Test
    void testReturnDistinct()
    {
        List<Long> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (y:Person)",
                    "  RETURN y.age AS age",
                    "}",
                    "RETURN DISTINCT age"
            );

            return tx.run( query ).stream().map( rec -> rec.get( "age" ).asLong() ).collect( Collectors.toList() );
        } );

        assertThat( r ).containsExactlyInAnyOrder( 30L, 40L );
    }

    @Test
    void testSubqueryWithNamespacerRenamedVariables()
    {
        List<Record> r = inTx( tx ->
        {
            // Namespacer will rename variables in the first local query
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "WITH x, 2 AS y",
                    "WITH x, y",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH y",
                    "  RETURN y AS z",
                    "}",
                    "RETURN x, y, z"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).get( "x" ).asLong() ).isEqualTo( 1L );
        assertThat( r.get( 0 ).get( "y" ).asLong() ).isEqualTo( 2L );
        assertThat( r.get( 0 ).get( "z" ).asLong() ).isEqualTo( 2L );
    }

    @Test
    void testSubqueryWithCreateAndReturn()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (f:Foo {name: 'abc'})",
                    "  RETURN f.name AS name",
                    "}",
                    "RETURN x, name"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "x", "name" );
        assertThat( r.get( 0 ).values() ).containsExactly( Values.value( 1 ), Values.value( "abc" ) );
    }

    @Test
    void testNameNormalization()
    {
        List<Record> r = inTx( tx ->
        {
            var query = "USE mega.graph(0) RETURN 1 AS x";

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "x" );
        assertThat( r.get( 0 ).values() ).containsExactly( Values.value( 1 ) );
    }

    @Test
    void testCorrelatedRemoteSubquery()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [10, 20] AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH x",
                    "  RETURN 1 + x AS y",
                    "}",
                    "RETURN x, y ORDER BY x"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 2 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "x", "y" );
        assertThat( r.get( 0 ).values() ).containsExactly( Values.value( 10 ), Values.value( 11 ) );
        assertThat( r.get( 1 ).keys() ).containsExactly( "x", "y" );
        assertThat( r.get( 1 ).values() ).containsExactly( Values.value( 20 ), Values.value( 21 ) );
    }

    @Test
    void testCorrelatedRemoteSubquerySupportedTypes()
    {
        List<Record> r = inTx( tx ->
        {
            var query = joinAsLines(
                    "WITH",
                    "  null AS nothing,",
                    "  true AS boolean,",
                    "  1 AS integer,",
                    "  3.14 AS float,",
                    "  'abc' AS string,",
                    "  [10, 20] AS list,",
                    "  {a: 1, b: 2} AS map,",
                    "  point({x: 1.0, y: 2.0}) AS point,",
                    "  datetime('2015-06-24T12:50:35.556+0100') AS datetime,",
                    "  localdatetime('2015185T19:32:24') AS localdatetime,",
                    "  date('+2015-W13-4') AS date,",
                    "  time('125035.556+0100') AS time,",
                    "  localtime('12:50:35.556') AS localtime,",
                    "  duration('PT16H12M') AS duration",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH",
                    "    nothing,",
                    "    boolean,",
                    "    integer,",
                    "    float,",
                    "    string,",
                    "    list,",
                    "    map,",
                    "    point,",
                    "    datetime,",
                    "    localdatetime,",
                    "    date,",
                    "    time,",
                    "    localtime,",
                    "    duration",
                    "  RETURN",
                    "    nothing AS nothing_2,",
                    "    boolean AS boolean_2,",
                    "    integer AS integer_2,",
                    "    float AS float_2,",
                    "    string AS string_2,",
                    "    list AS list_2,",
                    "    map AS map_2,",
                    "    point AS point_2,",
                    "    datetime AS datetime_2,",
                    "    localdatetime AS localdatetime_2,",
                    "    date AS date_2,",
                    "    time AS time_2,",
                    "    localtime AS localtime_2,",
                    "    duration AS duration_2",
                    "}",
                    "RETURN",
                    "  nothing_2,",
                    "  boolean_2,",
                    "  integer_2,",
                    "  float_2,",
                    "  string_2,",
                    "  list_2,",
                    "  map_2,",
                    "  point_2,",
                    "  datetime_2,",
                    "  localdatetime_2,",
                    "  date_2,",
                    "  time_2,",
                    "  localtime_2,",
                    "  duration_2"
            );

            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r.get( 0 ).get( "nothing_2" ) ).isEqualTo( Values.NULL );
        assertThat( r.get( 0 ).get( "boolean_2" ) ).isEqualTo( Values.value( true ) );
        assertThat( r.get( 0 ).get( "integer_2" ) ).isEqualTo( Values.value( 1L ) );
        assertThat( r.get( 0 ).get( "float_2" ) ).isEqualTo( Values.value( 3.14 ) );
        assertThat( r.get( 0 ).get( "string_2" ) ).isEqualTo( Values.value( "abc" ) );
        assertThat( r.get( 0 ).get( "list_2" ) ).isEqualTo( Values.value( List.of( 10L, 20L ) ) );
        assertThat( r.get( 0 ).get( "map_2" ) ).isEqualTo( Values.value( Map.of( "a", 1L, "b", 2L ) ) );
        assertThat( r.get( 0 ).get( "point_2" ) ).isIn( Values.point( 7203, 1.0, 2.0 ) );
        assertThat( r.get( 0 ).get( "datetime_2" ) ).isEqualTo( Values.value( ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" ) ) );
        assertThat( r.get( 0 ).get( "localdatetime_2" ) ).isEqualTo( Values.value( LocalDateTime.parse( "2015-07-04T19:32:24" ) ) );
        assertThat( r.get( 0 ).get( "date_2" ) ).isEqualTo( Values.value( LocalDate.parse( "2015-03-26" ) ) );
        assertThat( r.get( 0 ).get( "time_2" ) ).isEqualTo( Values.value( OffsetTime.parse( "12:50:35.556+01:00" ) ) );
        assertThat( r.get( 0 ).get( "localtime_2" ) ).isEqualTo( Values.value( LocalTime.parse( "12:50:35.556" ) ) );
        assertThat( r.get( 0 ).get( "duration_2" ) ).isEqualTo( Values.value( Duration.parse( "PT16H12M" ) ) );
    }

    @Test
    void testCorrelatedRemoteSubqueryNodeType()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (n:Test)",
                    "  RETURN n",
                    "}",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH n",
                    "  RETURN 1 AS x",
                    "}",
                    "RETURN n, x"
            );
            tx.run( query ).list();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "node values" );
        assertThat( ex.getMessage() ).containsIgnoringCase( "not supported" );
    }

    @Test
    void testCorrelatedRemoteSubqueryRelType()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (:Test)-[r:Rel]->(:Test)",
                    "  RETURN r",
                    "}",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH r",
                    "  RETURN 1 AS x",
                    "}",
                    "RETURN r, x"
            );
            tx.run( query ).list();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "relationship values" );
        assertThat( ex.getMessage() ).containsIgnoringCase( "not supported" );
    }

    @Test
    void testCorrelatedRemoteSubqueryPathType()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE p = (:T)-[:R]->(:T)",
                    "  RETURN p",
                    "}",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH p",
                    "  RETURN 1 AS x",
                    "}",
                    "RETURN p, x"
            );
            tx.run( query ).list();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "path values" );
        assertThat( ex.getMessage() ).containsIgnoringCase( "not supported" );
    }

    @Test
    void testPeriodicCommitShouldFail()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( tx ->
        {
            var query = joinAsLines(
                    "CYPHER planner=cost",
                    "USING PERIODIC COMMIT",
                    "WHAT EVER"
            );

            tx.run( query ).consume();
        } ) );

        assertThat( ex.getMessage() ).containsIgnoringCase( "periodic commit" );
    }

    @Test
    void testWriteInReadModeShouldFail()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
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
    void testCallReadProcedureOnAllShards()
    {
        List<String> result = inTx( AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "UNWIND mega.graphIds() AS g",
                    "CALL {",
                    "  USE mega.graph(g)",
                    "  CALL com.neo4j.utils.reader() YIELD foo",
                    "  RETURN foo",
                    "}",
                    "RETURN foo"
            );
            return tx.run( query ).stream().map( r -> r.get( "foo" ).asString() ).collect( Collectors.toList() );
        } );

        assertThat( result.size() ).isEqualTo( 2 );
        assertThat( result ).contains( "read", "read" );
    }

    @Test
    void testCallUnknownProcedureOnAllShards()
    {
        List<String> result = inTx( AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "UNWIND mega.graphIds() AS g",
                    "CALL {",
                    "  USE mega.graph(g)",
                    "  CALL com.neo4j.utils.readerOnShard() YIELD foo",
                    "  RETURN foo",
                    "}",
                    "RETURN foo"
            );
            return tx.run( query ).stream().map( r -> r.get( "foo" ).asString() ).collect( Collectors.toList() );
        } );

        assertThat( result.size() ).isEqualTo( 2 );
        assertThat( result ).contains( "read", "read" );
    }

    @Test
    void testCallUnknownProcedureOnAllShardsInWrite()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( AccessMode.WRITE, tx ->
        {
            var query = joinAsLines(
                    "UNWIND mega.graphIds() AS g",
                    "CALL {",
                    "  USE mega.graph(g)",
                    "  CALL com.neo4j.utils.readerOnShard() YIELD foo",
                    "  RETURN foo",
                    "}",
                    "RETURN foo"
            );
            tx.run( query ).consume();
        } ) );

        assertThat( ex.getMessage() ).contains( "Multi-shard writes not allowed" );
    }

    @Test
    void testCallWriteProcedureOnShardInRead()
    {
        ClientException ex = assertThrows( ClientException.class, () -> doInTx( AccessMode.READ, tx ->
        {
            var query = joinAsLines(
                    "USE mega.graph(0)",
                    "CALL com.neo4j.utils.writer() YIELD foo",
                    "RETURN foo"
            );
            tx.run( query ).consume();
        } ) );

        assertThat( ex.getMessage() ).contains( "Writing in read access mode not allowed" );
    }

    @Test
    void testQuerySummaryCounters()
    {
        ResultSummary r = inTx( tx ->
        {
            var query = joinAsLines(
                    "UNWIND [1, 2, 3] AS x",
                    "CALL {",
                    "  WITH x",
                    "  USE mega.graph(0)",
                    "  CREATE (n:T {p: x})",
                    "  RETURN n",
                    "}",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (m:T {p: 1})",
                    "  CREATE (m)-[r:R]->(x:X)",
                    "  SET x:Y, x.y = 10",
                    "  REMOVE x:Y",
                    "  REMOVE x.y",
                    "  DETACH DELETE m",
                    "  RETURN m",
                    "}",
                    "RETURN x"
            );

            return tx.run( query ).consume();
        } );

        assertThat( r.queryType() ).isEqualTo( QueryType.READ_WRITE );
        assertThat( r.counters().containsUpdates() ).isEqualTo( true );
        assertThat( r.counters().nodesCreated() ).isEqualTo( 4 );
        assertThat( r.counters().nodesDeleted() ).isEqualTo( 1 );
        assertThat( r.counters().relationshipsCreated() ).isEqualTo( 1 );
        assertThat( r.counters().relationshipsDeleted() ).isEqualTo( 1 );
        assertThat( r.counters().propertiesSet() ).isEqualTo( 5 );
        assertThat( r.counters().labelsAdded() ).isEqualTo( 5 );
        assertThat( r.counters().labelsRemoved() ).isEqualTo( 1 );
        assertThat( r.counters().indexesAdded() ).isEqualTo( 0 );
        assertThat( r.counters().indexesRemoved() ).isEqualTo( 0 );
        assertThat( r.counters().constraintsAdded() ).isEqualTo( 0 );
        assertThat( r.counters().constraintsRemoved() ).isEqualTo( 0 );
    }

    @Test
    void testBookmarks()
    {
        var b1 = runQuery( "USE mega.graph(0) CREATE ()", List.of() );
        verifyBookmark( b1, 0L );
        var b2 = runQuery( "USE mega.graph(1) CREATE ()", List.of() );
        verifyBookmark( b2, 1L );
        var b3 = runQuery( "USE mega.graph(0) CREATE ()", List.of() );
        verifyBookmark( b3, 0L );

        var b4 = runQuery( "RETURN 1", List.of( b1, b2, b3 ) );
        // check the information carried by b1, b2 and b3 has not been lost
        verifyBookmark( b4, 0L, 1L );
        verifyBookmarkComposition( b4, b1, b2, b3 );

        var b5 = runQuery( "USE mega.graph(1) CREATE ()", List.of( b1, b2, b3 ) );
        verifyBookmark( b5, 0L, 1L );
        verifyBookmarkComposition( b5, b1, b3 );

        var b6 = runQuery( "USE mega.graph(1) CREATE ()", List.of( b5 ) );
        verifyBookmark( b6, 0L, 1L );
        verifyBookmarkComposition( b6, b1, b3 );
    }

    private void verifyBookmark( Bookmark bookmark, Long... graphIds )
    {
        var internalBookmark = (InternalBookmark) bookmark;
        var parser = new FabricBookmarkParser();
        var fabricBookmark = parser.parse( Iterables.asList(internalBookmark.values()) ).stream()
                .map( b -> (FabricBookmark)b )
                .collect( Collectors.toList()).get( 0 );
        var graphStates = fabricBookmark.getGraphStates();
        assertEquals(graphIds.length, graphStates.size());
        var idsFromBookmark = graphStates.stream().map( FabricBookmark.GraphState::getRemoteGraphId ).collect( Collectors.toList());
        assertThat( idsFromBookmark ).contains( Arrays.stream( graphIds ).toArray( Long[]::new ) );
        assertTrue( graphStates.stream()
                .flatMap( gs -> gs.getBookmarks().stream() )
                .flatMap( b -> b.getSerialisedState().stream() ).noneMatch( String::isEmpty ) );
    }

    private void verifyBookmarkComposition( Bookmark compositeBookmark, Bookmark... bookmarks )
    {
        var remoteBookmarksFromComposite = parseBookmark( compositeBookmark );

        Arrays.stream( bookmarks )
                .flatMap( b -> parseBookmark( b ).stream() )
                .forEach( remoteBookmark -> assertTrue(remoteBookmarksFromComposite.contains( remoteBookmark )) );
    }

    private List<RemoteBookmark> parseBookmark( Bookmark bookmark )
    {
        var internalBookmark = (InternalBookmark) bookmark;
        var parser = new FabricBookmarkParser();
        return parser.parse( Iterables.asList(internalBookmark.values()) ).stream()
                .map( b -> (FabricBookmark) b )
                .flatMap( b -> b.getGraphStates().stream() )
                .flatMap( gs -> gs.getBookmarks().stream() )
                .collect( Collectors.toList() );
    }

    private Bookmark runQuery( String statement, List<Bookmark> bookmarks )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).withBookmarks( bookmarks ).build() ) )
        {
            var result = session.run( statement );
            result.consume();
            return session.lastBookmark();
        }
    }

    private <T> T inTx( Function<Transaction, T> workload )
    {
        return driverUtils.inTx( clientDriver, workload );
    }

    private void doInTx( Consumer<Transaction> workload )
    {
        driverUtils.doInTx( clientDriver, workload );
    }

    private <T> T inTx( AccessMode accessMode, Function<Transaction,T> workload )
    {
        return driverUtils.inTx( clientDriver, accessMode, workload );
    }

    private void doInTx( AccessMode accessMode, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( clientDriver, accessMode, workload );
    }
}
