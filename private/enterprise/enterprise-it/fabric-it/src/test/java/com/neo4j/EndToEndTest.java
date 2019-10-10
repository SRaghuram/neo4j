/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.types.Node;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EndToEndTest
{

    private static Driver clientDriver;
    private static TestServer testServer;
    private static InProcessNeo4j shard0;
    private static InProcessNeo4j shard1;
    private static Driver shard0Driver;
    private static Driver shard1Driver;

    @BeforeAll
    static void beforeAll() throws KernelException
    {

        shard0 = TestNeo4jBuilders.newInProcessBuilder().build();
        shard1 = TestNeo4jBuilders.newInProcessBuilder().build();

        PortUtils.Ports ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.graph.1.uri", shard1.boltURI().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
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
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = shard0Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" ).consume();
            tx.success();
        }
        try ( Transaction tx = shard1Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Person {name: 'Carrie', uid: 100, age: 30})" ).consume();
            tx.run( "CREATE (:Person {name: 'Dave'  , uid: 101, age: 90})" ).consume();
            tx.success();
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
    void testReadStrings()
    {
        List<String> result;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            result = Stream.concat(
                    tx.run( "USE mega.graph0 MATCH (n) RETURN n.name AS name" ).stream(),
                    tx.run( "USE mega.graph1 MATCH (n) RETURN n.name AS name" ).stream()
            ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( result, containsInAnyOrder( equalTo( "Anna" ), equalTo( "Bob" ), equalTo( "Carrie" ), equalTo( "Dave" ) ) );
    }

    @Test
    void testReadStringsFromView()
    {
        List<String> result;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            Map<String,Object> sid0 = Map.of( "sid", 0 );
            Map<String,Object> sid1 = Map.of( "sid", 1 );

            result = Stream.concat(
                    tx.run( "USE mega.graph($sid) MATCH (n) RETURN n.name AS name", sid0 ).stream(),
                    tx.run( "USE mega.graph($sid) MATCH (n) RETURN n.name AS name", sid1 ).stream()
            ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( result, containsInAnyOrder( equalTo( "Anna" ), equalTo( "Bob" ), equalTo( "Carrie" ), equalTo( "Dave" ) ) );
    }

    @Test
    void testReadNodes()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = Stream.concat(
                    tx.run( "USE mega.graph0 MATCH (n) RETURN n" ).stream(),
                    tx.run( "USE mega.graph1 MATCH (n) RETURN n" ).stream()
            ).map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        assertThat( labels, containsInAnyOrder( contains( "Person" ), contains( "Person" ), contains( "Person" ), contains( "Person" ) ) );

        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names, containsInAnyOrder( "Anna", "Bob", "Carrie", "Dave" ) );

    }

    @Test
    void testWriteNodes()
    {

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "USE mega.graph0 CREATE (:Cat {name: 'Whiskers'})" );
            tx.run( "USE mega.graph0 CREATE (:Cat {name: 'Charlie'})" );
            tx.success();
        }

        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "USE mega.graph1 CREATE (:Cat {name: 'Misty'})" );
            tx.run( "USE mega.graph1 CREATE (:Cat {name: 'Cupcake'})" );
            r = Stream.concat(
                    tx.run( "USE mega.graph0 MATCH (c:Cat) RETURN c" ).stream(),
                    tx.run( "USE mega.graph1 MATCH (c:Cat) RETURN c" ).stream()
            ).map( c -> c.get( "c" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        assertThat( labels, containsInAnyOrder( contains( "Cat" ), contains( "Cat" ), contains( "Cat" ), contains( "Cat" ) ) );
        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names, containsInAnyOrder( "Whiskers", "Charlie", "Misty", "Cupcake" ) );
    }

    @Test
    void testCustomShardKeyMapping()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            Map<String,Object> uid = Map.of( "uid", 100 );
            r = tx.run( String.join( "\n",
                    "USE mega.graph(com.neo4j.utils.personShard($uid))",
                    "MATCH (n {uid: $uid})",
                    "RETURN n"
            ), uid ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 0 ).get( "name" ).asString(), equalTo( "Carrie" ) );
        assertThat( r.get( 0 ).get( "uid" ).asInt(), equalTo( 100 ) );
    }

    @Test
    void testReadUnionAll()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "USE mega.graph0 MATCH (n) RETURN n",
                    "UNION ALL",
                    "USE mega.graph1 MATCH (n) RETURN n"
            ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        labels.forEach( l -> assertThat( l, contains( equalTo( "Person" ) ) ) );

        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names, containsInAnyOrder( "Anna", "Bob", "Carrie", "Dave" ) );
    }

    @Test
    void testReadUnionDistinct()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "USE mega.graph0 MATCH (n) RETURN n",
                    "UNION",
                    "USE mega.graph1 MATCH (n) RETURN n"
            ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        var labels = r.stream().map( Node::labels ).collect( Collectors.toList() );
        labels.forEach( l -> assertThat( l, contains( equalTo( "Person" ) ) ) );
        var names = r.stream().map( n -> n.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( names, containsInAnyOrder( "Anna", "Bob", "Carrie", "Dave" ) );
    }

    @Test
    void testReadUnionAllValues()
    {
        List<Integer> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "USE mega.graph0 MATCH (n) RETURN n.age AS a",
                    "UNION ALL",
                    "USE mega.graph1 MATCH (n) RETURN n.age AS a"
            ) ).stream().map( c -> c.get( "a" ).asInt() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r, containsInAnyOrder( equalTo( 30 ), equalTo( 30 ), equalTo( 40 ), equalTo( 90 ) ) );
    }

    @Test
    void testReadUnionDistinctValues()
    {
        List<Integer> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "USE mega.graph0 MATCH (n) RETURN n.age AS a",
                    "UNION",
                    "USE mega.graph1 MATCH (n) RETURN n.age AS a"
            ) ).stream().map( c -> c.get( "a" ).asInt() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r, containsInAnyOrder( equalTo( 30 ), equalTo( 40 ), equalTo( 90 ) ) );
    }

    @Test
    void testOptionalValue()
    {
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "USE mega.graph0 CREATE (:User {id:1}) - [:FRIEND] -> (:User)" );
            tx.success();
        }

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "USE mega.graph0 MATCH (n:User{id:1})-[:FRIEND]->(x:User) OPTIONAL MATCH (x)-[:FRIEND]->(y:User) RETURN x, y" ).consume();
            tx.success();
        }
    }

    @Test
    void testLocalSingleReturn()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( "RETURN 1+2 AS a, 'foo' AS f" ).list();
            tx.success();
        }

        assertThat( r.get( 0 ).get( "a" ).asInt(), equalTo( 3 ) );
        assertThat( r.get( 0 ).get( "f" ).asString(), equalTo( "foo" ) );
    }

    @Test
    void testReadFromShardWithProxyAliasing()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN x AS Sid, y AS Person" );
            r = tx.run( query ).list();
            tx.success();
        }

        List<Integer> shardIds = r.stream()
                .map( c -> c.get( "Sid" ).asInt() )
                .collect( Collectors.toList() );

        List<Node> persons = r.stream()
                .map( c -> c.get( "Person" ).asNode() )
                .collect( Collectors.toList() );

        assertThat( shardIds, equalTo( List.of( 0, 0, 1, 1 ) ) );

        assertThat( r.size(), equalTo( 4 ) );
        verifyPerson( persons, 0, "Anna" );
        verifyPerson( persons, 1, "Bob" );
        verifyPerson( persons, 2, "Carrie" );
        verifyPerson( persons, 3, "Dave" );
    }

    @Test
    void testAllGraphsRead()
    {
        List<Record> r;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND mega.graphIds() AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN gid, y AS person",
                    "  ORDER BY id(person)" );
            r = tx.run( query ).list();
            tx.success();
        }

        var graphIds = r.stream()
                .map( c -> c.get( "gid" ).asInt() )
                .collect( Collectors.toList() );

        var persons = r.stream()
                .map( c -> c.get( "person" ).asNode() )
                .collect( Collectors.toList() );

        assertThat( graphIds, equalTo( List.of( 0, 0, 1, 1 ) ) );

        assertThat( r.size(), equalTo( 4 ) );
        verifyPerson( persons, 0, "Anna" );
        verifyPerson( persons, 1, "Bob" );
        verifyPerson( persons, 2, "Carrie" );
        verifyPerson( persons, 3, "Dave" );
    }

    private void verifyPerson( List<Node> r, int index, String name )
    {
        assertThat( r.get( index ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( index ).get( "name" ).asString(), equalTo( name ) );
    }

    @Test
    void testIdTagging()
    {
        List<Record> r;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND mega.graphIds() AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  MATCH (p)",
                    "  RETURN p, id(p) AS local_id",
                    "}",
                    "RETURN gid, local_id, id(p) as tagged_id" );
            r = tx.run( query ).list();
            tx.success();
        }
        var gids = r.stream().map( c -> c.get( "gid" ).asLong() ).distinct().count();
        var local = r.stream().map( c -> c.get( "local_id" ).asLong() ).distinct().count();
        var tagged = r.stream().map( c -> c.get( "tagged_id" ).asLong() ).distinct().count();

        assertThat( gids, is( 2L ) );
        assertThat( local, allOf( greaterThanOrEqualTo( 2L ), lessThanOrEqualTo( 4L ) ) );
        assertThat( tagged, is( 4L ) );
    }

    @Test
    void testReadFromShardWithProxyOrdering()
    {
        List<String> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y",
                    "}",
                    "RETURN y.name AS name",
                    "ORDER BY name DESC" );

            List<Record> records = tx.run( query ).list();
            r = records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r, equalTo( List.of( "Dave", "Carrie", "Bob", "Anna" ) ) );
    }

    @Test
    void testReadFromShardWithProxyAggregation()
    {
        List<Record> records;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND [0, 1] AS x",
                    "CALL {",
                    "  USE mega.graph(x)",
                    "  MATCH (y)",
                    "  RETURN y.age AS age, y.name AS name",
                    "}",
                    "RETURN age, collect(name) AS names",
                    "ORDER BY age DESC" );

            records = tx.run( query ).list();
            tx.success();
        }

        assertThat( records.size(), is( 3 ) );
        assertThat( records.get( 0 ).keys(), contains( "age", "names" ) );
        assertThat( records.get( 0 ).get( 0 ).asInt(), is( 90 ) );
        assertThat( records.get( 0 ).get( 1 ).asList(), containsInAnyOrder( "Dave" ) );
        assertThat( records.get( 1 ).keys(), contains( "age", "names" ) );
        assertThat( records.get( 1 ).get( 0 ).asInt(), is( 40 ) );
        assertThat( records.get( 1 ).get( 1 ).asList(), containsInAnyOrder( "Bob" ) );
        assertThat( records.get( 2 ).keys(), contains( "age", "names" ) );
        assertThat( records.get( 2 ).get( 0 ).asInt(), is( 30 ) );
        assertThat( records.get( 2 ).get( 1 ).asList(), containsInAnyOrder( "Anna", "Carrie" ) );
    }

    @Test
    void testColumnJuggling()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
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

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).keys(), contains( "z", "w", "y", "x" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 3 ), Values.value( 4 ), Values.value( 2 ), Values.value( 20 ) ) );
    }

    @Test
    void testRemoteSubqueryInRemoteSubquery()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
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

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 3 ) );
        assertThat( r.get( 0 ).keys(), contains( "x", "y", "z", "w" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 1 ), Values.value( 10 ), Values.value( 100 ), Values.value( 1000 ) ) );
        assertThat( r.get( 1 ).values(), contains( Values.value( 2 ), Values.value( 20 ), Values.value( 200 ), Values.value( 2000 ) ) );
        assertThat( r.get( 2 ).values(), contains( Values.value( 3 ), Values.value( 30 ), Values.value( 300 ), Values.value( 3000 ) ) );
    }

    @Test
    void testSubqueryEndingWithCreate()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (:Foo)",
                    "}",
                    "RETURN x"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 0 ) );
    }

    @Test
    void testSubqueryEndingWithCreate2()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (f:Foo {name: 'abc'})",
                    "}",
                    "RETURN x"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 0 ) );
    }

    @Test
    void testSubqueryWithCreateAndReturn()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "WITH 1 AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  CREATE (f:Foo {name: 'abc'})",
                    "  RETURN f.name AS name",
                    "}",
                    "RETURN x, name"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).keys(), contains( "x", "name" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 1 ), Values.value( "abc" ) ) );
    }

    @Test
    void testCorrelatedRemoteSubquery()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND [10, 20] AS x",
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  WITH x",
                    "  RETURN 1 + x AS y",
                    "}",
                    "RETURN x, y"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 2 ) );
        assertThat( r.get( 0 ).keys(), contains( "x", "y" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 10 ), Values.value( 11 ) ) );
        assertThat( r.get( 1 ).keys(), contains( "x", "y" ) );
        assertThat( r.get( 1 ).values(), contains( Values.value( 20 ), Values.value( 21 ) ) );
    }

    @Test
    void testCorrelatedRemoteSubquerySupportedTypes()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
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

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).get( "nothing_2" ), is( Values.NULL ) );
        assertThat( r.get( 0 ).get( "boolean_2" ), is( Values.value( true ) ) );
        assertThat( r.get( 0 ).get( "integer_2" ), is( Values.value( 1L ) ) );
        assertThat( r.get( 0 ).get( "float_2" ), is( Values.value( 3.14 ) ) );
        assertThat( r.get( 0 ).get( "string_2" ), is( Values.value( "abc" ) ) );
        assertThat( r.get( 0 ).get( "list_2" ), is( Values.value( List.of( 10L, 20L ) ) ) );
        assertThat( r.get( 0 ).get( "map_2" ), is( Values.value( Map.of( "a", 1L, "b", 2L ) ) ) );
        assertThat( r.get( 0 ).get( "point_2" ), is( Values.point( 7203, 1.0, 2.0 ) ) );
        assertThat( r.get( 0 ).get( "datetime_2" ), is( Values.value( ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" ) ) ) );
        assertThat( r.get( 0 ).get( "localdatetime_2" ), is( Values.value( LocalDateTime.parse( "2015-07-04T19:32:24" ) ) ) );
        assertThat( r.get( 0 ).get( "date_2" ), is( Values.value( LocalDate.parse( "2015-03-26" ) ) ) );
        assertThat( r.get( 0 ).get( "time_2" ), is( Values.value( OffsetTime.parse( "12:50:35.556+01:00" ) ) ) );
        assertThat( r.get( 0 ).get( "localtime_2" ), is( Values.value( LocalTime.parse( "12:50:35.556" ) ) ) );
        assertThat( r.get( 0 ).get( "duration_2" ), is( Values.value( Duration.parse( "PT16H12M" ) ) ) );
    }

    @Test
    void testCorrelatedRemoteSubqueryNodeType()
    {
        ClientException ex = assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
            {
                var query = String.join( "\n",
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
                tx.success();
            }
        } );

        assertThat( ex.getMessage(), containsStringIgnoringCase( "node values" ) );
        assertThat( ex.getMessage(), containsStringIgnoringCase( "not supported" ) );
    }

    @Test
    void testCorrelatedRemoteSubqueryRelType()
    {
        ClientException ex = assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
            {
                var query = String.join( "\n",
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
                tx.success();
            }
        } );

        assertThat( ex.getMessage(), containsStringIgnoringCase( "relationship values" ) );
        assertThat( ex.getMessage(), containsStringIgnoringCase( "not supported" ) );
    }

    @Test
    void testCorrelatedRemoteSubqueryPathType()
    {
        ClientException ex = assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
            {
                var query = String.join( "\n",
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
                tx.success();
            }
        } );

        assertThat( ex.getMessage(), containsStringIgnoringCase( "path values" ) );
        assertThat( ex.getMessage(), containsStringIgnoringCase( "not supported" ) );
    }

    @Test
    void testPeriodicCommitShouldFail()
    {
        ClientException ex = assertThrows( ClientException.class, () ->
        {
            try ( Session s = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
            {
                var query = String.join( "\n",
                        "CYPHER planner=cost",
                        "USING PERIODIC COMMIT",
                        "WHAT EVER"
                );

                s.run( query ).consume();
            }
        } );

        assertThat( ex.getMessage(), containsStringIgnoringCase( "periodic commit" ) );
    }

    @Test
    void testQuerySummaryCounters()
    {
        ResultSummary r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
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

            r = tx.run( query ).summary();
            tx.success();
        }

        assertThat( r.statementType(), is( StatementType.READ_WRITE ) );
        assertThat( r.counters().containsUpdates(), is( true ) );
        assertThat( r.counters().nodesCreated(), is( 4 ) );
        assertThat( r.counters().nodesDeleted(), is( 1 ) );
        assertThat( r.counters().relationshipsCreated(), is( 1 ) );
        assertThat( r.counters().relationshipsDeleted(), is( 1 ) );
        assertThat( r.counters().propertiesSet(), is( 5 ) );
        assertThat( r.counters().labelsAdded(), is( 5 ) );
        assertThat( r.counters().labelsRemoved(), is( 1 ) );
        assertThat( r.counters().indexesAdded(), is( 0 ) );
        assertThat( r.counters().indexesRemoved(), is( 0 ) );
        assertThat( r.counters().constraintsAdded(), is( 0 ) );
        assertThat( r.counters().constraintsRemoved(), is( 0 ) );
    }
}
