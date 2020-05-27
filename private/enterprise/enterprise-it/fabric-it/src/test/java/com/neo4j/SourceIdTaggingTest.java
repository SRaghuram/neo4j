/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ShardFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.stream.SourceTagging;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@ExtendWith( FabricEverywhereExtension.class )
class SourceIdTaggingTest
{

    private static Driver mainDriver;
    private static TestServer testServer;
    private static Neo4j extA;
    private static Neo4j extB;
    private static Driver extADriver;
    private static Driver extBDriver;
    private static DriverUtils intA = new DriverUtils( "intA" );
    private static DriverUtils intB = new DriverUtils( "intB" );
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils fabric = new DriverUtils( "fabric" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll() throws KernelException
    {
        extA = Neo4jBuilders.newInProcessBuilder().withProcedure( ShardFunctions.class ).build();
        extB = Neo4jBuilders.newInProcessBuilder().withProcedure( ShardFunctions.class ).build();

        var configProperties = Map.of(
                "fabric.database.name", "fabric",
                "fabric.graph.0.uri", extA.boltURI().toString(),
                "fabric.graph.0.name", "extA",
                "fabric.graph.1.uri", extB.boltURI().toString(),
                "fabric.graph.1.name", "extB",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true" );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();
        testServer = new TestServer( config );

        // Un-comment to get debug log to console
        // testServer.setLogService( new SimpleLogService( new StdoutLogProvider() ) );

        testServer.start();

        mainDriver = driverTo( testServer.getBoltRoutingUri() );
        extADriver = driverTo( extA.boltURI() );
        extBDriver = driverTo( extB.boltURI() );

        doInTx( mainDriver, system, tx ->
        {
            tx.run( "CREATE DATABASE intA" ).consume();
            tx.run( "CREATE DATABASE intB" ).consume();
            tx.commit();
        } );
    }

    private static Driver driverTo( URI boltUri )
    {
        return GraphDatabase.driver(
                boltUri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
    }

    @BeforeEach
    void beforeEach()
    {
        doInTx( mainDriver, intA, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:A {name: 'nodeA'})-[:R {name: 'relR'}]->(:B {name: 'nodeB'})" ).consume();
            tx.commit();
        } );

        doInTx( mainDriver, intB, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:A {name: 'nodeA'})-[:R {name: 'relR'}]->(:B {name: 'nodeB'})" ).consume();
            tx.commit();
        } );

        doInTx( extADriver, neo4j, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:A {name: 'nodeA'})-[:R {name: 'relR'}]->(:B {name: 'nodeB'})" ).consume();
            tx.commit();
        } );

        doInTx( extBDriver, neo4j, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:A {name: 'nodeA'})-[:R {name: 'relR'}]->(:B {name: 'nodeB'})" ).consume();
            tx.commit();
        } );
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> mainDriver.close(),
                () -> extADriver.close(),
                () -> extBDriver.close(),
                () -> extA.close(),
                () -> extB.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void returnNodes()
    {
        var query = makeDoubleUnionQuery( "MATCH (x) RETURN x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 8 );
    }

    @Test
    void returnRelationships()
    {
        var query = makeDoubleUnionQuery( "MATCH ()-[x]-() RETURN x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 4 );
    }

    @Test
    void returnNodesAndRelationships()
    {
        var query = makeDoubleUnionQuery( "MATCH (s)-[r]->(e) RETURN s, r, e" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 4 );

        assertThat( records )
                .allSatisfy( rec ->
                             {
                                 assertThat( rec.get( "r" ).asRelationship().startNodeId() )
                                         .isEqualTo( rec.get( "s" ).asNode().id() );
                                 assertThat( rec.get( "r" ).asRelationship().endNodeId() )
                                         .isEqualTo( rec.get( "e" ).asNode().id() );
                             }
                );
    }

    @Test
    void returnPaths()
    {
        var query = makeDoubleUnionQuery( "MATCH x = ()-[]-() RETURN x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 8 );
    }

    @Test
    void returnDirectedPaths()
    {
        var query = makeDoubleUnionQuery( "MATCH x = ()-[]->() RETURN x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 4 );
    }

    @Test
    void returnListsOfNodes()
    {
        var query = makeDoubleUnionQuery( "MATCH (x) RETURN [x] AS x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 8 );
    }

    @Test
    void returnCollectNodes()
    {
        var query = makeDoubleUnionQuery( "MATCH (x) RETURN collect(x) AS x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 4 );
    }

    @Test
    void returnMapOfNodes()
    {
        var query = makeDoubleUnionQuery( "MATCH (x) RETURN {x:x} AS x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 8 );
    }

    @Test
    void returnMapOfCollectNodes()
    {
        var query = makeDoubleUnionQuery( "MATCH (x) RETURN {x:collect(x)} AS x" );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 4 );
    }

    @Test
    void aggregatingQuery()
    {
        var query = joinAsLines(
                "UNWIND [0, 1] AS n",
                "CALL { USE intA MATCH (x) RETURN x AS x1 }",
                "CALL { USE intB MATCH (x) RETURN x AS x2 }",
                "CALL { USE fabric.extA MATCH (x) RETURN x AS x3 }",
                "CALL { USE fabric.extB MATCH (x) RETURN x AS x4 }",
                "UNWIND [x1, x2, x3, x4] AS x",
                "RETURN DISTINCT x"
        );

        List<Record> records = run( fabric, query );

        assertThat( records )
                .hasSize( 8 );

        assertThat( records )
                .extracting( rec -> SourceTagging.extractSourceId( rec.get( "x" ).asNode().id() ) )
                .containsOnly( 0L, 1L, 3L, 4L );
    }

    private String makeDoubleUnionQuery( String query )
    {
        var graphs = List.of( "USE intA", "USE intB", "USE fabric.extA", "USE fabric.extB" );
        return Stream.concat( graphs.stream(), graphs.stream() )
                     .map( s -> s + " " + query )
                     .collect( Collectors.joining( " UNION " ) );
    }

    private static List<Record> run( DriverUtils context, String query )
    {
        return inTx( mainDriver, context, tx -> tx.run( query ).list() );
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