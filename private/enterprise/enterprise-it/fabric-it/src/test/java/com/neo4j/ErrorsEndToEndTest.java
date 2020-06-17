/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Set;

import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.api.exceptions.Status.Schema.ConstraintValidationFailed;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ArithmeticError;
import static org.neo4j.kernel.api.exceptions.Status.Statement.EntityNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ParameterMissing;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;

class ErrorsEndToEndTest
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
    void testUseErrorInEmbeddedApi()
    {
        var err = assertThrows( Exception.class, () -> testFabric.getTestServer()
                                                                 .getDbms()
                                                                 .database( "neo4j" )
                                                                 .executeTransactionally( "USE foo RETURN 1" ) );
        assertThat( err.getMessage() ).contains( "not available in embedded or http sessions" );
    }

    @Test
    void testParsingError()
    {
        var e = run( "Some Garbage"  );

        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage() ).contains( "Invalid input 'o'" );
        assertThat( e.getMessage() ).contains( "\"Some Garbage\"" );

        verifyCleanUp();
    }

    @Test
    void testSemanticError()
    {
        var e = run( "UNWIND[1, 0] AS a RETURN b" );

        // even though this error is reported as Syntax error to the user,
        // it is created during semantic analysis phase of query processing
        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e.getMessage() ).contains( "Variable `b` not defined" );
        assertThat( e.getMessage() ).contains( "\"UNWIND[1, 0] AS a RETURN b\"" );
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
        assertThat( e.getMessage() ).contains( "USE can only appear at the beginning of a (sub-)query" );
        assertThat( e.getMessage() ).contains( "\"USE mega.graph1\"" );
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
        assertThat( e.getMessage() ).contains( "USE can only appear at the beginning of a (sub-)query" );
        assertThat( e.getMessage() ).contains( "\"USE mega.graph0\"" );
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
                "UNWIND [0, 1, 5] AS gid",
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
        assertEquals( "Graph not found: 5", e.getMessage() );
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
        assertThat( e.getMessage() ).contains( "Unknown function 'somewhere.nonExistentFunction'" );
        assertThat( e.getMessage() ).contains( "\"USE mega.graph(somewhere.nonExistentFunction()) RETURN 1\"" );
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

    @Test
    void testKernelExceptionsMapping()
    {
        try ( var session = clientDriver.session() )
        {
            session.run( "CREATE CONSTRAINT ON (book:Library) ASSERT exists(book.isbn)" );
        }

        try ( var session = clientDriver.session() )
        {
            var tx = session.beginTransaction();
            tx.run( "CREATE (:Library)" );

            var e = assertThrows( ClientException.class, tx::commit );
            assertEquals( ConstraintValidationFailed.code().serialize(), e.code() );
            assertThat( e.getMessage() ).contains( "with label `Library` must have the property `isbn`" );
        }
    }

    private void verifyCleanUp()
    {
        verifyNoFabricTransactions();
        verifyNoRemoteTransactions();
    }

    private void verifyNoFabricTransactions()
    {
        driverUtils.doInTx( clientDriver, tx ->
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
        var db = (GraphDatabaseAPI) testFabric.getShard( 0 ).defaultDatabaseService();
        var activeTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertEquals( Set.of(), activeTransactions );
    }

    private ClientException run( String query )
    {
        return assertThrows( ClientException.class, () -> driverUtils.doInTx( clientDriver, tx -> tx.run( query ).list() ) );
    }
}
