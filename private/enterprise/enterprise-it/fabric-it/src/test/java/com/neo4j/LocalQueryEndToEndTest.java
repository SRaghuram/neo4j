/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class LocalQueryEndToEndTest
{
    private static Driver clientDriver;
    private static TestFabric testFabric;
    private static DriverUtils driverUtils;

    @BeforeAll
    static void setUp()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withShards( 1 )
                .build();

        var shardDriver = testFabric.driverForShard( 0 );

        try ( var session = shardDriver.session() )
        {
            session.run( "CREATE (:Person:Customer {name: 'Anna', uid: 0, age: 30})" );
            session.run( "CREATE (:Person:Employee {name: 'Bob',  uid: 1, age: 40})" );
            session.run( "CREATE (:Person:Employee {name: 'Carrie', uid: 2, age: 30})" );
            session.run( "CREATE (:Person:Customer {name: 'Dave'  , uid: 3})" );
            session.run( "CREATE (:Person:Customer {name: 'Eve'  , uid: 4, age: 45})" );
            session.run( "MATCH (e {name:'Bob'}), (c {name:'Anna'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-01-01')}]->(c)" );
            session.run( "MATCH (e {name:'Bob'}), (c {name:'Dave'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-02-01')}]->(c)" );
            session.run( "MATCH (e {name:'Carrie'}), (c {name:'Eve'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-03-01'), dummyMarker:true}]->(c)" );
            session.run( "MATCH (c {name:'Carrie'}), (b {name:'Bob'}) CREATE (c)-[:SUPERVISES]->(b)" );
        }

        clientDriver = testFabric.routingClientDriver();

        driverUtils = new DriverUtils( "mega" );
    }

    @AfterAll
    static void tearDown()
    {
        testFabric.close();
    }

    @Test
    void testPropertyAccess()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (e:Employee)-[r:RESPONSIBLE_FOR]->(c:Customer)",
                    "  RETURN e, r, c",
                    "}",
                    "WITH *",
                    "WHERE e.name = 'Bob' AND r.since > date('2019-01-30')",
                    "RETURN c.name AS name" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Dave" ) );
    }

    // AndedInequalities is a special AST construct that works with properties
    @Test
    void testPropertyAndedInequalities()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (e:Employee)-[r:RESPONSIBLE_FOR]->(c:Customer)",
                    "  RETURN e, r, c",
                    "}",
                    "WITH *",
                    "WHERE r.since >= date('2019-02-01') AND r.since < date('2019-03-01')",
                    "RETURN c.name AS name" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Dave" ) );
    }

    @Test
    void testCachedPropertyAccess()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (e:Employee)-[r:RESPONSIBLE_FOR]->(c:Customer)",
                    "  RETURN e, r, c",
                    "}",
                    "WITH *",
                    "WHERE e.name <> 'Carrie' AND r.since > date('2019-01-30')",
                    "RETURN e.name AS name, r.since AS since" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Bob" ) );
    }

    @Test
    void testLabel()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (x)",
                    "  RETURN x",
                    "}",
                    "WITH *",
                    "WHERE x:Employee ",
                    "RETURN x.name AS name",
                    "ORDER BY name" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Bob", "Carrie" ) );
    }

    @Test
    void testNodePropertyExists()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (n)",
                    "  RETURN n",
                    "}",
                    "WITH *",
                    "WHERE NOT EXISTS(n.age) ",
                    "RETURN n.name AS name" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Dave" ) );
    }

    @Test
    void testRelationshipPropertyExists()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (n)-[r]->()",
                    "  RETURN n, r",
                    "}",
                    "WITH *",
                    "WHERE EXISTS(r.dummyMarker) ",
                    "RETURN n.name AS name" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .map( c -> c.get( "name" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "Carrie" ) );
    }

    @Test
    void testNodeKeys()
    {
        List<String> r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (e:Employee)-[r:RESPONSIBLE_FOR]->(c:Customer)",
                    "  RETURN e, r, c",
                    "}",
                    "WITH *",
                    "RETURN keys(c) AS keys" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .flatMap( c -> c.get( "keys" ).asList().stream() )
                    .map( o -> (String) o )
                    .distinct()
                    .sorted()
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "age", "name", "uid" ) );
    }

    @Test
    void testRelationshipKeys()
    {
        List<String> r = inMegaTx( tx -> {
            var query = joinAsLines(
                    "CALL {",
                    "  USE mega.graph(0)",
                    "  MATCH (e:Employee)-[r:RESPONSIBLE_FOR]->(c:Customer)",
                    "  RETURN e, r, c",
                    "}",
                    "WITH *",
                    "RETURN keys(r) AS keys" );

            List<Record> records = tx.run( query ).list();
            return records.stream()
                    .flatMap( c -> c.get( "keys" ).asList().stream() )
                    .map( o -> (String) o )
                    .distinct()
                    .sorted()
                    .collect( Collectors.toList() );
        } );

        assertThat( r ).isEqualTo( List.of( "dummyMarker", "since" ) );
    }

    private <T> T inMegaTx( Function<Transaction, T> workload )
    {
        return driverUtils.inTx( clientDriver, workload );
    }
}
