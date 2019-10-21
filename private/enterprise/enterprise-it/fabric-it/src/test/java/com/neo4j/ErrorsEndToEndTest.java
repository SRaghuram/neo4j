/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
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
    private static InProcessNeo4j remote;

    @BeforeAll
    static void setUp()
    {

        remote = TestNeo4jBuilders.newInProcessBuilder()
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
                "dbms.connector.bolt.enabled", "true");
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
        try ( var tx = begin() )
        {
            tx.run( "Some Garbage" ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                 "Invalid input 'o': expected 'h/H', 't/T' or 'e/E' (line 1, column 2 (offset: 1))",
                    "\"Some Garbage\"",
                    "  ^"
            );

            assertEquals( SyntaxError.code().serialize(), e.code() );
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }

        verifyCleanUp();
    }

    @Test
    void testSemanticError()
    {
        try ( var tx = begin() )
        {
            tx.run( "UNWIND[1, 0] AS a RETURN b" ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "Variable `b` not defined (line 1, column 26 (offset: 25))",
                    "\"UNWIND[1, 0] AS a RETURN b\"",
                    "                          ^"
            );

            // even though this error is reported as Syntax error to the user,
            // it is created during semantic analysis phase of query processing
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testLocalDDL()
    {
        try ( var tx = begin() )
        {
            tx.run(  "CREATE USER me SET PASSWORD 'secret1234'" ).list();
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            var expectedMessage = "This is an administration command and it should be executed against the system database: CREATE USER";
            assertEquals(NotSystemDatabaseError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testRemoteDDL()
    {
        try ( var tx = begin() )
        {
            tx.run(  "USE mega.graph0 CREATE USER me SET PASSWORD 'secret1234'" ).list();
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "Invalid input ' ': expected 'r/R' (line 1, column 31 (offset: 30))",
                    "\"USE mega.graph0 CREATE USER me SET PASSWORD 'secret1234'\"",
                    "                               ^"
            );
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testLocalCommand()
    {
        try ( var tx = begin() )
        {
            tx.run(  "CREATE INDEX ON :Person(firstname)" ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = "Commands not supported in Fabric database";
            assertEquals(SemanticError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testRemoteCommand()
    {
        try ( var tx = begin() )
        {
            tx.run(  "USE mega.graph0 CREATE INDEX ON :Person(firstname)" ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "Invalid input 'N': expected 'p/P' (line 1, column 31 (offset: 30))",
                    "\"USE mega.graph0 CREATE INDEX ON :Person(firstname)\"",
                    "                               ^"
            );
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testMultipleUseInRoutingQuery()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "USE mega.graph0",
                    "UNWIND[1, 0] AS a",
                    "USE mega.graph1",
                    "RETURN a"
            );

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "USE can only appear at the beginning of a (sub-)query",
                    "\"USE mega.graph1\"",
                    "     ^"
            );
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testUSEInMiddleOfRoutingQuery()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "UNWIND[1, 0] AS a",
                    "USE mega.graph0",
                    "RETURN a"
            );

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "USE can only appear at the beginning of a (sub-)query",
                    "\"USE mega.graph0\"",
                    "     ^"
            );
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testRoutingQueryUseEvaluationError()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "USE mega.graph2",
                    "UNWIND[1, 0] AS a",
                    "RETURN a"
            );

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(EntityNotFound.code().serialize(), e.code());
            assertEquals( "Catalog entry not found: mega.graph2", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testSubqueryUseEvaluationError()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "UNWIND [0, 1] AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  MATCH (c:Customer)",
                    "  RETURN c",
                    "}",
                    "WITH *",
                    "RETURN c.name AS name");

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(EntityNotFound.code().serialize(), e.code());
            assertEquals("Catalog entry not found: mega.graph1", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testSubqueryUseEvaluationError2()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "UNWIND [1, 0] AS a",
                    "CALL {",
                    "  USE mega.graph((1 - a)/a)",
                    "  MATCH (c:Customer)",
                    "  RETURN c",
                    "}",
                    "WITH *",
                    "RETURN c.name AS name");

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(ArithmeticError.code().serialize(), e.code());
            assertEquals("/ by zero", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringEvaluationOfLocalQuery()
    {
        try ( var tx = begin() )
        {
            var query = "RETURN $a";

            tx.run( query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(ParameterMissing.code().serialize(), e.code());
            assertEquals("Expected parameter(s): a", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringStreamingOfLocalQuery()
    {
        try ( var tx = begin() )
        {
            var query = "UNWIND[1, 0] AS a RETURN 1/a AS aa";

            tx.run( query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(ArithmeticError.code().serialize(), e.code());
            assertEquals("/ by zero", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testShardingFunctionNotFound()
    {
        try ( var tx = begin() )
        {
            var query = "USE mega.graph(somewhere.nonExistentFunction()) RETURN 1";

            tx.run( query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            var expectedMessage = String.join(  "\n",
                    "Unknown function 'somewhere.nonExistentFunction'",
                    "\"USE mega.graph(somewhere.nonExistentFunction()) RETURN 1\"",
                    "                ^"
            );
            assertEquals(SyntaxError.code().serialize(), e.code());
            assertEquals(expectedMessage, e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringEvaluationOfRoutingQuery()
    {
        try ( var tx = begin() )
        {
            var query = "USE mega.graph0 RETURN $a";

            tx.run( query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(ParameterMissing.code().serialize(), e.code());
            assertEquals("Expected parameter(s): a", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringStreamingOfRoutingQuery()
    {
        try ( var tx = begin() )
        {
            var query = "USE mega.graph0 UNWIND[1, 0] AS a RETURN 1/a AS aa";

            tx.run( query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals( ArithmeticError.code().serialize(), e.code() );
            assertEquals( "/ by zero", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringEvaluationOfRemoteSubQuery()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "UNWIND [0] AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  RETURN $a",
                    "}",
                    "RETURN *");

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals(ParameterMissing.code().serialize(), e.code());
            assertEquals( "Expected parameter(s): a", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    @Test
    void testErrorDuringStreamingOfRemoteSubQuery()
    {
        try ( var tx = begin() )
        {
            var query = String.join( "\n",
                    "UNWIND [0] AS gid",
                    "CALL {",
                    "  USE mega.graph(gid)",
                    "  UNWIND[1, 0] AS a",
                    "  MATCH (c:Customer)",
                    "  RETURN c, 1/a AS aa",
                    "}",
                    "WITH *",
                    "RETURN c.name AS name, aa");

            tx.run(  query ).list();
            fail("Exception expected");
        }
        catch ( ClientException e )
        {
            assertEquals( ArithmeticError.code().serialize(), e.code() );
            assertEquals("/ by zero", e.getMessage());
        }
        catch ( Exception e )
        {
            fail("Unexpected exception: " + e);
        }
    }

    private Transaction begin()
    {
        return clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction();
    }

    private void verifyCleanUp()
    {
        verifyNoFabricTransactions();
        verifyNoRemoteTransactions();
    }

    private void verifyNoFabricTransactions()
    {
        try ( var tx = begin() )
        {
            var openTransactions = tx.run( "CALL dbms.listTransactions() YIELD transactionId, currentQuery,status" ).list();
            if ( !openTransactions.isEmpty() )
            {
                fail( "There are some transactions open in Fabric DB: " + openTransactions );
            }
        }
    }

    private void verifyNoRemoteTransactions()
    {
        var db = (GraphDatabaseAPI) remote.defaultDatabaseService();
        var activeTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertEquals( Set.of(), activeTransactions );
    }
}
