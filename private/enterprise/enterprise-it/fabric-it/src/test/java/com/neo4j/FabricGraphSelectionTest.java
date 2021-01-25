/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.server.security.auth.AuthProcedures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class FabricGraphSelectionTest
{

    private static Driver mainDriver;
    private static TestFabric testFabric;
    private static Driver extADriver;
    private static DriverUtils intA = new DriverUtils( "intA" );
    private static DriverUtils intB = new DriverUtils( "intB" );
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils fabric = new DriverUtils( "fabric" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "fabric" )
                .withShards( "extA" )
                // Un-comment to get debug log to console
                //.withLogService( new SimpleLogService( new StdoutLogProvider() ) )
                .registerFuncOrProc( AuthProcedures.class )
                .registerFuncOrProc( ProxyFunctions.class )
                .build();

        mainDriver = testFabric.routingClientDriver();

        extADriver = testFabric.driverForShard( 0 );

        doInTx( mainDriver, system, tx ->
        {
            tx.run( "CREATE DATABASE intA" ).consume();
            tx.run( "CREATE DATABASE intB" ).consume();
            tx.commit();
        } );
    }

    @BeforeEach
    void beforeEach()
    {
        doInTx( mainDriver, system, tx ->
        {
            tx.run( "DROP USER myUser IF EXISTS" ).consume();
            tx.commit();
        } );

        doInTx( mainDriver, neo4j, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Identity {name: 'neo4j'})" ).consume();
            tx.commit();
        } );

        doInTx( mainDriver, intA, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Identity {name: 'intA'})" ).consume();
            tx.commit();
        } );

        doInTx( mainDriver, intB, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.run( "CREATE (:Identity {name: 'intB'})" ).consume();
            tx.commit();
        } );

        doInTx( extADriver, neo4j, tx ->
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:Identity {name: 'extA'})" ).consume();
            tx.commit();
        } );
    }

    @AfterAll
    static void afterAll()
    {
        testFabric.close();
    }

    @Test
    void plainDefault()
    {
        var query = joinAsLines( "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .isEmpty();

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .isNotNull();
    }

    @Test
    void useOnly()
    {
        var query = joinAsLines( "USE intA" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .isNotNull();

        assertThat( catchThrowable( () -> run( fabric, query ) ) )
                .isNotNull();

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .isNotNull();
    }

    @Test
    void plainSingleExplicitInternal1()
    {
        var query = joinAsLines( "USE intA",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intA" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intA" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intA" );
    }

    @Test
    void plainSingleExplicitInternal2()
    {
        var query = joinAsLines( "USE intB",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    void plainSingleExplicitExternal()
    {
        var query = joinAsLines( "USE fabric.extA",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );
    }

    @Test
    void unionSameGraphDefault()
    {
        var query = joinAsLines( "MATCH (n) RETURN n",
                                 "  UNION",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .isEmpty();

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .isNotNull();
    }

    @Test
    void unionSameGraphExplicit()
    {
        var query = joinAsLines( "USE intB",
                                 "MATCH (n) RETURN n",
                                 "  UNION",
                                 "USE intB",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    void unionExplicitDefaultAndDefault()
    {
        var query = joinAsLines( "USE neo4j",
                                 "MATCH (n) RETURN n",
                                 "  UNION",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph system (transaction default)" );
    }

    @Test
    void unionDefaultAndExplicitDefault()
    {
        var query = joinAsLines( "MATCH (n) RETURN n",
                                 "  UNION",
                                 "USE neo4j",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph neo4j" );
    }

    @Test
    void unionAllDefaultAndExplicitDefault()
    {
        var query = joinAsLines( "MATCH (n) RETURN n",
                                 "  UNION ALL",
                                 "USE neo4j",
                                 "MATCH (n) RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j", "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph neo4j" );
    }

    @Test
    void unionExplicitAndDefault()
    {
        var query = joinAsLines( "USE intB",
                                 "MATCH (n) RETURN n",
                                 "  UNION",
                                 "MATCH (n) RETURN n" );

        assertThat( catchThrowableOfType( () -> run( neo4j, query ), ClientException.class ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph neo4j (transaction default)" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph system (transaction default)" );
    }

    @Test
    @Disabled
        //TODO: Fix source graph tagging for local database execution
    void unionExplicitAndExplicit()
    {
        var query = joinAsLines( "USE fabric.extA",
                                 "MATCH (n) RETURN n",
                                 "  UNION",
                                 "USE intB",
                                 "MATCH (n) RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "extA", "intB" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );
    }

    @Test
    void unionAllExplicitAndExplicit()
    {
        var query = joinAsLines( "USE fabric.extA",
                                 "MATCH (n) RETURN n",
                                 "  UNION ALL",
                                 "USE intB",
                                 "MATCH (n) RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "extA", "intB" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );
    }

    @Test
    void unionDefaultAndDynamic()
    {
        var query = joinAsLines( "MATCH (n) RETURN n",
                                 "  UNION",
                                 "USE fabric.graph(0)",
                                 "MATCH (n) RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph(0)" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph(0)" );
    }

    @Test
    void unionDynamicAndDefault()
    {
        var query = joinAsLines( "USE fabric.graph($g)",
                                 "MATCH (n) RETURN n",
                                 "  UNION",
                                 "MATCH (n) RETURN n" );

        var params = Map.<String,Object>of( "g", 0 );

        assertThat( catchThrowable( () -> run( neo4j, query, params ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph($g)" );

        assertThat( run( fabric, query, params ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( catchThrowable( () -> run( system, query, params ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph($g)" );
    }

    @Test
    void subqueryDefaultAndInherited()
    {
        var query = joinAsLines( "CALL {",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "neo4j" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .isEmpty();

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .isNotNull();
    }

    @Test
    void subqueryExplicitAndInherited()
    {
        var query = joinAsLines( "USE intB",
                                 "CALL {",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    void subqueryExplicitAndExplicit()
    {
        var query = joinAsLines( "USE intB",
                                 "CALL {",
                                 "  USE intB",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    void subqueryDefaultAndExplicit()
    {
        var query = joinAsLines( "CALL {",
                                 "  USE intB",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Multiple graphs in the same query not allowed" )
                .hasMessageContaining( "Attempted to access graph intB" );
    }

    @Test
    void subqueryDefaultAndDynamic()
    {
        var query = joinAsLines( "CALL {",
                                 "  USE fabric.graph(0)",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph(0)" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Dynamic graph lookup not allowed here" )
                .hasMessageContaining( "Attempted to access graph fabric.graph(0)" );
    }

    @Test
    void commandDefault()
    {
        var query = joinAsLines( "CREATE INDEX FOR (n:Foo) ON (n.p)" );

        assertThat( run( neo4j, query ) ).isNotNull();
        assertThat( catchThrowable( () -> run( fabric, query ) ) )
                .hasMessageContaining( "Schema operations are not allowed for user '' with FULL restricted to ACCESS." );
        assertThat( catchThrowable( () -> run( system, query ) ) )
                .hasMessageContaining( "Not a recognised system command or procedure. This Cypher command can only be executed in a user database" );
    }

    @Test
    void commandExplicit()
    {
        var queryCreate = joinAsLines( "USE intB", "CREATE INDEX myIndex FOR (n:Foo) ON (n.p)" );
        var queryDrop = "DROP INDEX myIndex";

        assertThat( run( neo4j, queryCreate ) ).isNotNull();
        run( intB, queryDrop );
        assertThat( run( fabric, queryCreate ) ).isNotNull();
        run( intB, queryDrop );
        assertThat( run( system, queryCreate ) ).isNotNull();
        run( intB, queryDrop );
    }

    @Test
    void systemDefault()
    {
        var query = joinAsLines( "SHOW DATABASES" );

        assertThat( run( neo4j, query ) ).isNotNull();
        assertThat( run( fabric, query ) ).isNotNull();
        assertThat( run( system, query ) ).isNotNull();
    }

    @Test
    void systemExplicitSystem()
    {
        var query = joinAsLines( "USE system",
                                 "SHOW DATABASES" );

        assertThat( catchThrowable( () -> run( neo4j, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
        assertThat( catchThrowable( () -> run( fabric, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
        assertThat( catchThrowable( () -> run( system, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
    }

    @Test
    void systemExplicitOther()
    {
        var query = joinAsLines( "USE intB",
                                 "SHOW DATABASES" );

        assertThat( catchThrowable( () -> run( neo4j, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
        assertThat( catchThrowable( () -> run( fabric, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
        assertThat( catchThrowable( () -> run( system, query )) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
    }

    @Test
    void systemProcedureCall()
    {
        var query = joinAsLines( "USE system",
                                 "CALL dbms.procedures" );

        assertThat( run( neo4j, query ) ).isNotNull();
        assertThat( run( fabric, query ) ).isNotNull();
        assertThat( run( system, query ) ).isNotNull();
    }

    @Test
    void systemSecurityProcedureCall()
    {
        var queryCreate = joinAsLines( "USE system",
                "CALL dbms.security.createUser('myUser', 'secret')" );
        var queryDrop = "DROP USER myUser";

        assertThat( run( neo4j, queryCreate ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();

        assertThat( run( fabric, queryCreate ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();

        assertThat( run( system, queryCreate ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();
    }

    @Test
    void systemSecurityProcedureCallWithParameter()
    {
        var queryCreate = joinAsLines( "USE system",
                "CALL dbms.security.createUser($myUsername, 'secret')" );
        var queryDrop = "DROP USER myUser";

        assertThat( run( neo4j, queryCreate, Map.of("myUsername", "myUser") ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();

        assertThat( run( fabric, queryCreate, Map.of("myUsername", "myUser") ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();

        assertThat( run( system, queryCreate, Map.of("myUsername", "myUser") ) ).isNotNull();
        assertThat( run( system, queryDrop ) ).isNotNull();
    }

    @Test
    void systemSecurityProcedureCallFailWhenNotOnSystem()
    {
        var queryCreate = "CALL dbms.security.createUser('myUser', 'secret')";

        assertThat( catchThrowable( () -> run( neo4j, queryCreate ) ) ).hasMessageContaining(
                "This is an administration command and it should be executed against the system database: dbms.security.createUser" );

        assertThat( catchThrowable( () -> run( fabric, queryCreate ) ) ).hasMessageContaining(
                "This is an administration command and it should be executed against the system database: dbms.security.createUser" );

        assertThat( run( system, queryCreate ) ).isNotNull();
    }

    @Test
    void systemProcedureCallVoid()
    {
        var query = joinAsLines( "USE system",
                                 "CALL db.awaitIndexes" );

        assertThat( run( neo4j, query ) ).isNotNull();
        assertThat( run( fabric, query ) ).isNotNull();
        assertThat( run( system, query ) ).isNotNull();
    }

    @Test
    void fabricPure()
    {
        var query = joinAsLines( "USE fabric",
                                 "RETURN 1 AS a" );

        assertThat( run( neo4j, query ) )
                .extracting( intValue( "a" ) )
                .containsExactly( 1 );

        assertThat( run( fabric, query ) )
                .extracting( intValue( "a" ) )
                .containsExactly( 1 );

        assertThat( run( system, query ) )
                .extracting( intValue( "a" ) )
                .containsExactly( 1 );
    }

    @Test
    void fabricSubqueryExplicit()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE intB",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    void fabricSubqueryExplicitQualifiedExternal()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE fabric.extA",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "extA" );
    }

    @Test
    @Disabled( "Should we support fabric.intB?" )
    void fabricSubqueryExplicitQualifiedInternal()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE fabric.intB",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactly( "intB" );
    }

    @Test
    @Disabled
    //TODO: Fix source graph tagging for local database execution
    void fabricSubqueryUnionExplicitAndExplicit()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE intB",
                                 "  MATCH (n) RETURN n",
                                 "    UNION",
                                 "  USE fabric.extA",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );
    }

    @Test
    void fabricSubqueryUnionAllExplicitAndExplicit()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE intB",
                                 "  MATCH (n) RETURN n",
                                 "    UNION ALL",
                                 "  USE fabric.extA",
                                 "  MATCH (n) RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( run( neo4j, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );

        assertThat( run( fabric, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );

        assertThat( run( system, query ) )
                .extracting( nodeProperty( "n", "name" ) )
                .containsExactlyInAnyOrder( "intB", "extA" );
    }

    @Test
    void fabricSubqueryExplicitSubqueryExplicit()
    {
        var query = joinAsLines( "USE fabric",
                                 "CALL {",
                                 "  USE extA",
                                 "  CALL {",
                                 "      USE intB",
                                 "      MATCH (n) RETURN n",
                                 "  }",
                                 "  RETURN n",
                                 "}",
                                 "RETURN n" );

        assertThat( catchThrowable( () -> run( neo4j, query ) ) )
                .isNotNull();

        assertThat( catchThrowable( () -> run( fabric, query ) ) )
                .isNotNull();

        assertThat( catchThrowable( () -> run( system, query ) ) )
                .isNotNull();
    }

    @Test
    void standaloneProcedureCall()
    {
        var query = joinAsLines( "USE intA",
                "CALL com.neo4j.utils.reader()" );

        assertThat( run( neo4j, query ) )
                .extracting( record -> record.get( "foo" ).asString() )
                .containsExactly( "read" );

        assertThat( run( fabric, query ) )
                .extracting( record -> record.get( "foo" ).asString() )
                .containsExactly( "read" );

        assertThat( run( system, query ) )
                .extracting( record -> record.get( "foo" ).asString() )
                .containsExactly( "read" );
    }

    private static List<Record> run( DriverUtils context, String query )
    {
        return inTx( mainDriver, context, tx -> tx.run( query ).list() );
    }

    private static List<Record> run( DriverUtils context, String query, Map<String,Object> params )
    {
        return inTx( mainDriver, context, tx -> tx.run( query, params ).list() );
    }

    private static <T> T inTx( Driver driver, DriverUtils driverUtils, Function<Transaction,T> workload )
    {
        return driverUtils.inTx( driver, workload );
    }

    private static void doInTx( Driver driver, DriverUtils driverUtils, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( driver, workload );
    }

    private static Function<Record,String> nodeProperty( String column, String property )
    {
        return rec -> rec.get( column ).asNode().get( property ).asString();
    }

    private static Function<Record,Integer> intValue( String column )
    {
        return rec -> rec.get( column ).asInt();
    }
}
