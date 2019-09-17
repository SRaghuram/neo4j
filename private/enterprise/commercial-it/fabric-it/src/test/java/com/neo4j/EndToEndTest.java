/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.neo4j.driver.types.Node;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EndToEndTest
{

    private Driver clientDriver;
    private TestServer testServer;
    private InProcessNeo4j shard0;
    private InProcessNeo4j shard1;

    @BeforeEach
    void setUp() throws KernelException
    {

        shard0 = TestNeo4jBuilders.newInProcessBuilder()
                .withFixture( "CREATE (:Person {name: 'Anna', uid: 0, age: 30})" )
                .withFixture( "CREATE (:Person {name: 'Bob',  uid: 1, age: 40})" )
                .build();
        shard1 = TestNeo4jBuilders.newInProcessBuilder()
                .withFixture( "CREATE (:Person {name: 'Carrie', uid: 100, age: 30})" )
                .withFixture( "CREATE (:Person {name: 'Dave'  , uid: 101, age: 90})" )
                .build();

        PortUtils.Ports ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.graph.1.uri", shard1.boltURI().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
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
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        clientDriver.close();
        shard0.close();
        shard1.close();
    }

    @Test
    void testReadStrings()
    {
        List<String> result;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            result = Stream.concat(
                    tx.run( "FROM mega.graph0 MATCH (n) RETURN n.name AS name" ).stream(),
                    tx.run( "FROM mega.graph1 MATCH (n) RETURN n.name AS name" ).stream()
            ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( result, contains( equalTo( "Anna" ), equalTo( "Bob" ), equalTo( "Carrie" ), equalTo( "Dave" ) ) );
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
                    tx.run( "FROM mega.graph($sid) MATCH (n) RETURN n.name AS name", sid0 ).stream(),
                    tx.run( "FROM mega.graph($sid) MATCH (n) RETURN n.name AS name", sid1 ).stream()
            ).map( r -> r.get( "name" ).asString() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( result, contains( equalTo( "Anna" ), equalTo( "Bob" ), equalTo( "Carrie" ), equalTo( "Dave" ) ) );
    }

    @Test
    void testReadNodes()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = Stream.concat(
                    tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).stream(),
                    tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).stream()
            ).map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        assertThat( r.get( 0 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 0 ).get( "name" ).asString(), equalTo( "Anna" ) );
        assertThat( r.get( 1 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 1 ).get( "name" ).asString(), equalTo( "Bob" ) );
        assertThat( r.get( 2 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 2 ).get( "name" ).asString(), equalTo( "Carrie" ) );
        assertThat( r.get( 3 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 3 ).get( "name" ).asString(), equalTo( "Dave" ) );
    }

    @Test
    void testWriteNodes()
    {

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE  ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "FROM mega.graph0 CREATE (:Cat {name: 'Whiskers'})" );
            tx.run( "FROM mega.graph0 CREATE (:Cat {name: 'Charlie'})" );
            tx.success();
        }

        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE  ).withDatabase( "mega" ).build() )
                .beginTransaction() )
        {
            tx.run( "FROM mega.graph1 CREATE (:Cat {name: 'Misty'})" );
            tx.run( "FROM mega.graph1 CREATE (:Cat {name: 'Cupcake'})" );
            r = Stream.concat(
                    tx.run( "FROM mega.graph0 MATCH (c:Cat) RETURN c" ).stream(),
                    tx.run( "FROM mega.graph1 MATCH (c:Cat) RETURN c" ).stream()
            ).map( c -> c.get( "c" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        assertThat( r.get( 0 ).labels(), contains( equalTo( "Cat" ) ) );
        assertThat( r.get( 0 ).get( "name" ).asString(), equalTo( "Whiskers" ) );
        assertThat( r.get( 1 ).labels(), contains( equalTo( "Cat" ) ) );
        assertThat( r.get( 1 ).get( "name" ).asString(), equalTo( "Charlie" ) );
        assertThat( r.get( 2 ).labels(), contains( equalTo( "Cat" ) ) );
        assertThat( r.get( 2 ).get( "name" ).asString(), equalTo( "Misty" ) );
        assertThat( r.get( 3 ).labels(), contains( equalTo( "Cat" ) ) );
        assertThat( r.get( 3 ).get( "name" ).asString(), equalTo( "Cupcake" ) );
    }

    @Test
    void testCustomShardKeyMapping()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            Map<String,Object> uid = Map.of( "uid", 100 );
            r = tx.run( String.join( "\n",
                    "FROM mega.graph(com.neo4j.utils.personShard($uid))",
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
                    "FROM mega.graph0 MATCH (n) RETURN n",
                    "UNION ALL",
                    "FROM mega.graph1 MATCH (n) RETURN n"
            ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        assertThat( r.get( 0 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 0 ).get( "name" ).asString(), equalTo( "Anna" ) );
        assertThat( r.get( 1 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 1 ).get( "name" ).asString(), equalTo( "Bob" ) );
        assertThat( r.get( 2 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 2 ).get( "name" ).asString(), equalTo( "Carrie" ) );
        assertThat( r.get( 3 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 3 ).get( "name" ).asString(), equalTo( "Dave" ) );
    }

    @Test
    void testReadUnionDistinct()
    {
        List<Node> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "FROM mega.graph0 MATCH (n) RETURN n",
                    "UNION",
                    "FROM mega.graph1 MATCH (n) RETURN n"
            ) ).stream().map( c -> c.get( "n" ).asNode() ).collect( Collectors.toList() );
            tx.success();
        }

        assertThat( r.size(), equalTo( 4 ) );
        assertThat( r.get( 0 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 0 ).get( "name" ).asString(), equalTo( "Anna" ) );
        assertThat( r.get( 1 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 1 ).get( "name" ).asString(), equalTo( "Bob" ) );
        assertThat( r.get( 2 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 2 ).get( "name" ).asString(), equalTo( "Carrie" ) );
        assertThat( r.get( 3 ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( 3 ).get( "name" ).asString(), equalTo( "Dave" ) );
    }

    @Test
    void testReadUnionAllValues()
    {
        List<Integer> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            r = tx.run( String.join( "\n",
                    "FROM mega.graph0 MATCH (n) RETURN n.age AS a",
                    "UNION ALL",
                    "FROM mega.graph1 MATCH (n) RETURN n.age AS a"
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
                    "FROM mega.graph0 MATCH (n) RETURN n.age AS a",
                    "UNION",
                    "FROM mega.graph1 MATCH (n) RETURN n.age AS a"
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
            tx.run( "FROM mega.graph0 CREATE (:User {id:1}) - [:FRIEND] -> (:User)" );
            tx.success();
        }

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mega" ).build()  )
                .beginTransaction() )
        {
            tx.run( "FROM mega.graph0 MATCH (n:User{id:1})-[:FRIEND]->(x:User) OPTIONAL MATCH (x)-[:FRIEND]->(y:User) RETURN x, y" ).consume();
            tx.success();
        }
    }

    @Test
    void testLocalSingleReturn()
    {
        List<Record> r;

        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build()  ).beginTransaction() )
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
                    "  FROM mega.graph(x)",
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
                    "  FROM mega.graph(gid)",
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

    @Test
    void testIdTagging()
    {
        List<Record> r;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            var query = String.join( "\n",
                    "UNWIND mega.graphIds() AS gid",
                    "CALL {",
                    "  FROM mega.graph(gid)",
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
        assertThat( local, is( 2L ) );
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
                    "  FROM mega.graph(x)",
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
                    "  FROM mega.graph(x)",
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
                    "  FROM mega.graph(0)",
                    "  CREATE (a: A)",
                    "}",
                    "CALL {",
                    "  WITH 0 AS a",
                    "  RETURN 4 AS w",
                    "}",
                    "RETURN z, w, y"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).keys(), contains( "z", "w", "y" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 3 ), Values.value( 4 ), Values.value( 2 ) ) );
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
                    "  FROM mega.graph(0)",
                    "  CREATE (:Foo)",
                    "}",
                    "RETURN x"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).keys(), contains( "x" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 1 ) ) );
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
                    "  FROM mega.graph(0)",
                    "  CREATE (f:Foo {name: 'abc'})",
                    "}",
                    "RETURN x"
            );

            r = tx.run( query ).list();
            tx.success();
        }

        assertThat( r.size(), equalTo( 1 ) );
        assertThat( r.get( 0 ).keys(), contains( "x" ) );
        assertThat( r.get( 0 ).values(), contains( Values.value( 1 ) ) );
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
                    "  FROM mega.graph(0)",
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

    private void verifyPerson( List<Node> r, int index, String name )
    {
        assertThat( r.get( index ).labels(), contains( equalTo( "Person" ) ) );
        assertThat( r.get( index ).get( "name" ).asString(), equalTo( name ) );
    }
}
