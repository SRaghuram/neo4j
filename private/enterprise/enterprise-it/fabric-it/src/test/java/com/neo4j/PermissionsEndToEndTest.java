/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.api.exceptions.Status.Security.Forbidden;

class PermissionsEndToEndTest
{

    private static Driver adminDriver;
    private static Driver readerDriver;
    private static Driver driverWithoutRole;
    private static TestServer testServer;
    private static InProcessNeo4j remote;

    @BeforeAll
    static void setUp() throws KernelException
    {

        remote = TestNeo4jBuilders.newInProcessBuilder()
                .build();
        var ports = PortUtils.findFreePorts();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", remote.boltURI().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true",
                "dbms.security.auth_enabled", "true" );
        var config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( CustomFunctions.class );

        try ( var initDriver = createDriver( "neo4j", "neo4j", ports.bolt ) )
        {
            try ( var tx = begin( initDriver, "system" ) )
            {
                tx.run( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '1234'" ).list();
                tx.success();
            }
        }

        adminDriver = createDriver( "neo4j", "1234", ports.bolt );

        try ( var tx = adminDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ).beginTransaction() )
        {
            tx.run( "CREATE USER userWithReaderPermission SET PASSWORD '1234' CHANGE NOT REQUIRED" ).consume();
            tx.run( "GRANT ROLE reader TO userWithReaderPermission" ).consume();
            tx.run( "CREATE USER userWithNoPermission SET PASSWORD '1234' CHANGE NOT REQUIRED" ).consume();
            tx.success();
        }

        readerDriver = createDriver( "userWithReaderPermission", "1234", ports.bolt );
        driverWithoutRole = createDriver( "userWithNoPermission", "1234", ports.bolt );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.stop();
        if ( adminDriver != null )
        {
            adminDriver.close();
        }

        if ( readerDriver != null )
        {
            readerDriver.close();
        }

        if ( driverWithoutRole != null )
        {
            driverWithoutRole.close();
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
    void testReaderCanUseFabric()
    {
        try ( var tx = begin( readerDriver, "mega" ) )
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

    @Disabled
    @Test
    void testUserWithoutReadPermissionsCannotUseFunctionsInUseClause()
    {
        try ( var tx = begin( driverWithoutRole, "mega" ) )
        {
            var query = "USE mega.graph(com.neo4j.utils.myPlusOne(-1)) RETURN 1";
            tx.run( query ).consume();
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( Forbidden.code().serialize(), e.code() );
            assertEquals( "Database access is not allowed for user 'userWithNoPermission' with roles [].", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    @Test
    void testUserWithoutReadPermissionsCannotUseFunctionsInLocalQueries()
    {
        try ( var tx = begin( driverWithoutRole, "mega" ) )
        {
            var query = "RETURN com.neo4j.utils.myPlusOne(-1)";
            tx.run( query ).consume();
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( Forbidden.code().serialize(), e.code() );
            assertEquals( "Database access is not allowed for user 'userWithNoPermission' with roles [].", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    @Test
    void testAdminCannotWriteToFabricDatabase()
    {
        try ( var tx = begin( adminDriver, "mega" ) )
        {
            var query = "CREATE (n)";
            tx.run( query ).consume();
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( Forbidden.code().serialize(), e.code() );
            assertEquals( "Write operations are not allowed for user 'neo4j' with roles [admin] restricted to READ.", e.getMessage() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception: " + e );
        }
    }

    private static Driver createDriver( String user, String password, int boltPort )
    {
        return GraphDatabase.driver(
                "neo4j://localhost:" + boltPort,
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
