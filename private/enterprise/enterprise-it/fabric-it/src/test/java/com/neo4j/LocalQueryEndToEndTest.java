/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class LocalQueryEndToEndTest
{
    private Driver clientDriver;
    private TestServer testServer;
    private Neo4j shard0;
    private DriverUtils driverUtils;

    @BeforeEach
    void setUp() throws KernelException
    {

        shard0 = Neo4jBuilders.newInProcessBuilder()
                .withFixture( "CREATE (:Person:Customer {name: 'Anna', uid: 0, age: 30})" )
                .withFixture( "CREATE (:Person:Employee {name: 'Bob',  uid: 1, age: 40})" )
                .withFixture( "CREATE (:Person:Employee {name: 'Carrie', uid: 2, age: 30})" )
                .withFixture( "CREATE (:Person:Customer {name: 'Dave'  , uid: 3})" )
                .withFixture( "CREATE (:Person:Customer {name: 'Eve'  , uid: 4, age: 45})" )
                .withFixture( "MATCH (e {name:'Bob'}), (c {name:'Anna'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-01-01')}]->(c)" )
                .withFixture( "MATCH (e {name:'Bob'}), (c {name:'Dave'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-02-01')}]->(c)" )
                .withFixture( "MATCH (e {name:'Carrie'}), (c {name:'Eve'}) CREATE (e)-[:RESPONSIBLE_FOR {since: date('2019-03-01'), dummyMarker:true}]->(c)" )
                .withFixture( "MATCH (c {name:'Carrie'}), (b {name:'Bob'}) CREATE (c)-[:SUPERVISES]->(b)" )
                .build();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true" );
        var config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProcedures.class )
                .registerFunction( ProxyFunctions.class );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );

        driverUtils = new DriverUtils( "mega" );
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        clientDriver.close();
        shard0.close();
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
