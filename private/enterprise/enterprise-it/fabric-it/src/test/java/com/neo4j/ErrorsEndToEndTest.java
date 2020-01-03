/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.utils.DriverUtils.doInMegaTx;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ArithmeticError;
import static org.neo4j.kernel.api.exceptions.Status.Statement.EntityNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Statement.NotSystemDatabaseError;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ParameterMissing;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SemanticError;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;

class ErrorsEndToEndTest
{

    private static Driver clientDriver;
    private static TestServer testServer;
    private static Neo4j remote;

    @BeforeAll
    static void setUp()
    {

        remote = Neo4jBuilders.newInProcessBuilder()
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
        var ports = PortUtils.findFreePorts();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", remote.boltURI().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true" );
        var config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        clientDriver = GraphDatabase.driver(
                "neo4j://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.stop();
        clientDriver.close();
        remote.close();
    }

    @Test
    void testParsingError()
    {
        var e = run( "Some Garbage"  );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "Invalid input 'o'" ) );
        assertThat( e.getMessage(), containsString( "\"Some Garbage\"" ) );

        verifyCleanUp();
    }

    @Test
    void testSemanticError()
    {
        var e = run( "UNWIND[1, 0] AS a RETURN b" );

        // even though this error is reported as Syntax error to the user,
        // it is created during semantic analysis phase of query processing
        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "Variable `b` not defined" ) );
        assertThat( e.getMessage(), containsString( "\"UNWIND[1, 0] AS a RETURN b\"" ) );
    }

    @Test
    void testLocalDDL()
    {
        var e = run( "CREATE USER me SET PASSWORD 'secret1234'" );

        var expectedMessage = "This is an administration command and it should be executed against the system database: CREATE USER";
        assertEquals( NotSystemDatabaseError.code().serialize(), e.code() );
        assertEquals( expectedMessage, e.getMessage() );
    }

    @Test
    void testRemoteDDL()
    {
        var e = run( "USE mega.graph0 CREATE USER me SET PASSWORD 'secret1234'" );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "Invalid input ' '" ) );
        assertThat( e.getMessage(), containsString( "\"USE mega.graph0 CREATE USER me SET PASSWORD 'secret1234'\"" ) );
    }

    @Test
    void testLocalCommand()
    {
        var e = run( "CREATE INDEX ON :Person(firstname)" );

        var expectedMessage = "Commands not supported in Fabric database";
        assertEquals( SemanticError.code().serialize(), e.code() );
        assertEquals( expectedMessage, e.getMessage() );
    }

    @Test
    void testRemoteCommand()
    {
        var e = run( "USE mega.graph0 CREATE INDEX ON :Person(firstname)" );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "Invalid input 'N'" ) );
        assertThat( e.getMessage(), containsString( "\"USE mega.graph0 CREATE INDEX ON :Person(firstname)\"" ) );
    }

    @Test
    void testMultipleUseInRoutingQuery()
    {
        var query = joinAsLines(
                "USE mega.graph0",
                "UNWIND[1, 0] AS a",
                "USE mega.graph1",
                "RETURN a"
        );

        var e = run( query );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "USE can only appear at the beginning of a (sub-)query" ) );
        assertThat( e.getMessage(), containsString( "\"USE mega.graph1\"" ) );
    }

    @Test
    void testUSEInMiddleOfRoutingQuery()
    {
        var query = joinAsLines(
                "UNWIND[1, 0] AS a",
                "USE mega.graph0",
                "RETURN a"
        );

        var e = run( query );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "USE can only appear at the beginning of a (sub-)query" ) );
        assertThat( e.getMessage(), containsString( "\"USE mega.graph0\"" ) );
    }

    @Test
    void testRoutingQueryUseEvaluationError()
    {
        var query = joinAsLines(
                "USE mega.graph2",
                "UNWIND[1, 0] AS a",
                "RETURN a"
        );

        var e = run( query );

        assertEquals( EntityNotFound.code().serialize(), e.code() );
        assertEquals( "Catalog entry not found: mega.graph2", e.getMessage() );
    }

    @Test
    void testSubqueryUseEvaluationError()
    {
        var query = joinAsLines(
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  MATCH (c:Customer)",
                "  RETURN c",
                "}",
                "WITH *",
                "RETURN c.name AS name"
        );

        var e = run( query );

        assertEquals( EntityNotFound.code().serialize(), e.code() );
        assertEquals( "Graph not found: 1", e.getMessage() );
    }

    @Test
    void testSubqueryUseEvaluationError2()
    {
        var query = joinAsLines(
                "UNWIND [1, 0] AS a",
                "CALL {",
                "  USE mega.graph((1 - a)/a)",
                "  MATCH (c:Customer)",
                "  RETURN c",
                "}",
                "WITH *",
                "RETURN c.name AS name"
        );

        var e = run( query );

        assertEquals( ArithmeticError.code().serialize(), e.code() );
        assertEquals( "/ by zero", e.getMessage() );
    }

    @Test
    void testErrorDuringEvaluationOfLocalQuery()
    {
        var e = run(  "RETURN $a" );

        assertEquals( ParameterMissing.code().serialize(), e.code() );
        assertEquals( "Expected parameter(s): a", e.getMessage() );
    }

    @Test
    void testErrorDuringStreamingOfLocalQuery()
    {
        var e = run( "UNWIND[1, 0] AS a RETURN 1/a AS aa" );

        assertEquals( ArithmeticError.code().serialize(), e.code() );
        assertEquals( "/ by zero", e.getMessage() );
    }

    @Test
    void testShardingFunctionNotFound()
    {
        var e = run( "USE mega.graph(somewhere.nonExistentFunction()) RETURN 1" );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage(), containsString( "Unknown function 'somewhere.nonExistentFunction'" ) );
        assertThat( e.getMessage(), containsString( "\"USE mega.graph(somewhere.nonExistentFunction()) RETURN 1\"" ) );
    }

    @Test
    void testErrorDuringEvaluationOfRoutingQuery()
    {
        var e = run("USE mega.graph(0) RETURN $a" );

        assertEquals( ParameterMissing.code().serialize(), e.code() );
        assertEquals( "Expected parameter(s): a", e.getMessage() );
    }

    @Test
    void testErrorDuringStreamingOfRoutingQuery()
    {
        var e = run( "USE mega.graph(0) UNWIND[1, 0] AS a RETURN 1/a AS aa" );

        assertEquals( ArithmeticError.code().serialize(), e.code() );
        assertEquals( "/ by zero", e.getMessage() );
    }

    @Test
    void testErrorDuringEvaluationOfRemoteSubQuery()
    {
        var query = joinAsLines(
                "UNWIND [0] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  RETURN $a",
                "}",
                "RETURN *" );

        var e = run( query );

        assertEquals( ParameterMissing.code().serialize(), e.code() );
        assertEquals( "Expected parameter(s): a", e.getMessage() );
    }

    @Test
    void testErrorDuringStreamingOfRemoteSubQuery()
    {
        var query = joinAsLines(
                "UNWIND [0] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  UNWIND[1, 0] AS a",
                "  MATCH (c:Customer)",
                "  RETURN c, 1/a AS aa",
                "}",
                "WITH *",
                "RETURN c.name AS name, aa" );

        var e = run( query );

        assertEquals( ArithmeticError.code().serialize(), e.code() );
        assertEquals( "/ by zero", e.getMessage() );
    }

    private void verifyCleanUp()
    {
        verifyNoFabricTransactions();
        verifyNoRemoteTransactions();
    }

    private void verifyNoFabricTransactions()
    {
        doInMegaTx( clientDriver, tx ->
        {
            var openTransactions = tx.run( "CALL dbms.listTransactions() YIELD transactionId, currentQuery,status" ).list();
            if ( !openTransactions.isEmpty() )
            {
                fail( "There are some transactions open in Fabric DB: " + openTransactions );
            }
        } );
    }

    private void verifyNoRemoteTransactions()
    {
        var db = (GraphDatabaseAPI) remote.defaultDatabaseService();
        var activeTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertEquals( Set.of(), activeTransactions );
    }

    private ClientException run( String query )
    {
        return assertThrows( ClientException.class, () -> doInMegaTx( clientDriver, tx -> tx.run( query ).list() ) );
    }
}
