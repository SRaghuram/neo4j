/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.api.exceptions.Status.Security.Forbidden;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ExecutionFailed;

class PermissionsEndToEndTest
{

    private static Driver adminDriver;
    private static Driver accessDriver;
    private static Driver noPermissionDriver;
    private static TestServer testServer;
    private static Neo4j remote;

    @BeforeAll
    static void setUp() throws KernelException
    {

        remote = Neo4jBuilders.newInProcessBuilder()
                .build();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", remote.boltURI().toString(),
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true",
                "dbms.security.auth_enabled", "true" );
        var config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( ProxyFunctions.class );

        try ( var initDriver = createDriver( "neo4j", "neo4j" ) )
        {
            try ( var tx = begin( initDriver, "system" ) )
            {
                tx.run( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '1234'" ).list();
                tx.commit();
            }
        }

        adminDriver = createDriver( "neo4j", "1234" );

        try ( var tx = adminDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ).beginTransaction() )
        {
            tx.run( "CREATE ROLE access" ).consume();
            tx.run( "GRANT ACCESS ON DATABASE * TO access" ).consume();
            tx.run( "CREATE USER userWithAccessPermission SET PASSWORD '1234' CHANGE NOT REQUIRED" ).consume();
            tx.run( "GRANT ROLE access TO userWithAccessPermission" ).consume();

            tx.run( "CREATE USER userWithNoPermission SET PASSWORD '1234' CHANGE NOT REQUIRED" ).consume();
            tx.commit();
        }

        accessDriver = createDriver( "userWithAccessPermission", "1234" );
        noPermissionDriver = createDriver( "userWithNoPermission", "1234" );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.stop();
        if ( adminDriver != null )
        {
            adminDriver.close();
        }

        if ( accessDriver != null )
        {
            accessDriver.close();
        }

        if ( noPermissionDriver != null )
        {
            noPermissionDriver.close();
        }

        remote.close();
    }

    @Test
    void testAdminCanUseFabric()
    {
        try ( var tx = begin( adminDriver, "mega" ) )
        {
            var query = joinAsLines(
                    "UNWIND [0] AS gid",
                    "CALL {",
                    "  USE mega.graph(com.neo4j.utils.myPlusOne(gid -1))",
                    "  RETURN 1",
                    "}",
                    "RETURN com.neo4j.utils.myPlusOne(1)" );

            var result = tx.run( query );
            assertEquals( 1, result.list().size() );
        }
    }

    @Test
    void testUserWithAccessCanUseFabric()
    {
        try ( var tx = begin( accessDriver, "mega" ) )
        {
            var query = joinAsLines(
                    "UNWIND [0] AS gid",
                    "CALL {",
                    "  USE mega.graph(com.neo4j.utils.myPlusOne(gid -1))",
                    "  RETURN 1",
                    "}",
                    "RETURN com.neo4j.utils.myPlusOne(1)" );

            var result = tx.run( query );
            assertEquals( 1, result.list().size() );
        }
    }

    @Test
    void testWithoutPermissionsCannotUseFabric()
    {
        var e = assertThrows( ClientException.class, () ->
        {
            try ( var tx = begin( noPermissionDriver, "mega" ) )
            {
                var query = "RETURN 1";
                tx.run( query ).consume();
            }
        } );

        assertEquals( Forbidden.code().serialize(), e.code() );
        assertEquals( "Database access is not allowed for user 'userWithNoPermission' with roles [PUBLIC].", e.getMessage() );
    }

    @Test
    void testAdminCanUseLocalMatch()
    {
        try ( var tx = begin( adminDriver, "mega" ) )
        {
            var query = "MATCH (n) RETURN n";
            var result = tx.run( query );
            assertEquals( 0, result.list().size() );
        }
    }

    @Test
    void testUserWithAccessCanUseLocalMatch()
    {
        try ( var tx = begin( accessDriver, "mega" ) )
        {
            var query = "MATCH (n) RETURN n";
            var result = tx.run( query );
            assertEquals( 0, result.list().size() );
        }
    }

    @Test
    void testAdminCannotWriteToFabricDatabase()
    {
        var e = assertThrows( ClientException.class, () ->
        {
            try ( var tx = begin( adminDriver, "mega" ) )
            {
                var query = "CREATE (n)";
                tx.run( query ).consume();
            }
        } );

        assertEquals( Forbidden.code().serialize(), e.code() );
        assertEquals( "Create node with labels '' is not allowed for user 'neo4j' with roles [PUBLIC, admin] restricted to ACCESS.", e.getMessage() );
    }

    @Test
    void testUserWithAccessCannotWriteToFabricDatabase()
    {
        var e = assertThrows( ClientException.class, () ->
        {
            try ( var tx = begin( accessDriver, "mega" ) )
            {
                var query = "CREATE (n)";
                tx.run( query ).consume();
            }
        } );

        assertEquals( Forbidden.code().serialize(), e.code() );
        assertEquals( "Create node with labels '' is not allowed for user 'userWithAccessPermission' with roles [PUBLIC, access] restricted to ACCESS.",
                      e.getMessage() );
    }

    @Test
    void testAdminCannotAccessDataInUse()
    {
        var e = assertThrows( DatabaseException.class, () ->
        {
            try ( var tx = begin( adminDriver, "mega" ) )
            {
                var query = "USE mega.graph(size([p = (x)-->(y) | p])) RETURN 1";
                tx.run( query ).consume();
            }
        } );

        assertEquals( ExecutionFailed.code().serialize(), e.code() );
        assertEquals( "`PatternComprehension` should have been rewritten away", e.getMessage() );
    }

    private static Driver createDriver( String user, String password )
    {
        return GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.basic( user, password ),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    private static Transaction begin( Driver driver, String database )
    {
        return driver.session( SessionConfig.builder().withDatabase( database ).build() ).beginTransaction();
    }
}
