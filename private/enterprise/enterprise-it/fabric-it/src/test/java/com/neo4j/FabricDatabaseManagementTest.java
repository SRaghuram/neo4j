/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;

class FabricDatabaseManagementTest
{
    private TestServer testServer;
    private Path databaseDir;

    @BeforeEach
    void setUp() throws IOException
    {
        databaseDir = Files.createTempDirectory( getClass().getSimpleName() );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        FileUtils.deletePathRecursively( databaseDir );
        stopServer();
    }

    void createServer( String fabricDatabaseName )
    {
        Map<String,String> configProperties = Map.of();
        if ( fabricDatabaseName != null )
        {
            configProperties = Map.of( "fabric.database.name", fabricDatabaseName );
        }

        var config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config, databaseDir );

        testServer.start();
    }

    @Test
    void testDatabaseLifecycle()
    {
        createServer( null );

        try ( var tx = openSystemDbTransaction() )
        {
            var fabricDatabases = getFabricDatabases( tx );
            assertTrue( fabricDatabases.isEmpty() );
        }

        stopServer();

        createServer( "mega" );

        try ( var tx = openSystemDbTransaction() )
        {
            var fabricDatabases = getFabricDatabases( tx );
            assertEquals( 1, fabricDatabases.size() );
            var mega = fabricDatabases.get( 0 );
            assertEquals( "mega", mega.getProperty( DATABASE_NAME_PROPERTY ) );
            assertEquals( "online", mega.getProperty( DATABASE_STATUS_PROPERTY ) );
        }

        stopServer();

        createServer( "giga" );

        try ( var tx = openSystemDbTransaction() )
        {
            var fabricDatabases = getFabricDatabases( tx );
            assertEquals( 2, fabricDatabases.size() );
            var mega = getDb( fabricDatabases, "mega" );
            assertNotNull( mega );
            assertEquals( "offline", mega.getProperty( DATABASE_STATUS_PROPERTY ) );

            var giga = getDb( fabricDatabases, "giga" );
            assertNotNull( giga );
            assertEquals( "online", giga.getProperty( DATABASE_STATUS_PROPERTY ) );
        }

        stopServer();

        createServer( null );

        try ( var tx = openSystemDbTransaction() )
        {
            var fabricDatabases = getFabricDatabases( tx );
            assertEquals( 2, fabricDatabases.size() );
            var mega = getDb( fabricDatabases, "mega" );
            assertNotNull( mega );
            assertEquals( "offline", mega.getProperty( DATABASE_STATUS_PROPERTY ) );

            var giga = getDb( fabricDatabases, "giga" );
            assertNotNull( giga );
            assertEquals( "offline", giga.getProperty( DATABASE_STATUS_PROPERTY ) );
        }

        stopServer();

        createServer( "mega" );

        try ( var tx = openSystemDbTransaction() )
        {
            var fabricDatabases = getFabricDatabases( tx );
            assertEquals( 2, fabricDatabases.size() );
            var mega = getDb( fabricDatabases, "mega" );
            assertNotNull( mega );
            assertEquals( "online", mega.getProperty( DATABASE_STATUS_PROPERTY ) );

            var giga = getDb( fabricDatabases, "giga" );
            assertNotNull( giga );
            assertEquals( "offline", giga.getProperty( DATABASE_STATUS_PROPERTY ) );
        }

        stopServer();
    }

    private Node getDb( List<Node> fabricDatabases, String name )
    {
        return fabricDatabases.stream().filter( db -> db.getProperty( DATABASE_NAME_PROPERTY ).equals( name ) ).findAny().orElse( null );
    }

    private Transaction openSystemDbTransaction()
    {
        var dbms = testServer.getDependencies().resolveDependency( DatabaseManagementService.class );
        var systemDb = dbms.database( "system" );
        return systemDb.beginTx();
    }

    private List<Node> getFabricDatabases( Transaction tx )
    {
        Function<ResourceIterator<Node>,List<Node>> iterator = nodes ->
        {
            List<Node> fabricDatabases = new ArrayList<>();
            while ( nodes.hasNext() )
            {
                Node fabricDb = nodes.next();
                fabricDatabases.add( fabricDb );
            }
            nodes.close();
            return fabricDatabases;
        };

        return iterator.apply( tx.findNodes( DATABASE_LABEL, "fabric", true ) );
    }

    private void stopServer()
    {
        if ( testServer != null )
        {
            testServer.stop();
            testServer = null;
        }
    }
}
