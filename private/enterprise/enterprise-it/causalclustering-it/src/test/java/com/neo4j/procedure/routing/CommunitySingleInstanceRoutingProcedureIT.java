/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.routing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class CommunitySingleInstanceRoutingProcedureIT extends BaseRoutingProcedureIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldContainRoutingProcedure()
    {
        SocketAddress advertisedBoltAddress = new SocketAddress( "neo4j.com", 7687 );
        db = startDb( advertisedBoltAddress );

        RoutingResult expectedResult = newRoutingResult( advertisedBoltAddress );

        assertRoutingProceduresAvailable( db, expectedResult );
    }

    @Test
    void shouldUseClientProvidedAddressOverride()
    {
        SocketAddress advertisedBoltAddress = new SocketAddress( "neo4j.com", 7687 );
        db = startDb( advertisedBoltAddress );
        var clientProvidedAddress = new SocketAddress( "foo.com", 9999 );

        RoutingResult expectedResult = newRoutingResult( clientProvidedAddress );

        assertRoutingProceduresAvailable( db, expectedResult, Map.of( "address", clientProvidedAddress.toString() ) );
    }

    @Test
    void shouldAllowRoutingDriverToReadAndWrite() throws IOException
    {
        db = startDb();

        assertPossibleToReadAndWriteUsingRoutingDriver( boltAddress() );
    }

    @Test
    void shouldCallRoutingProcedureWithValidDatabaseName()
    {
        String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
        SocketAddress advertisedBoltAddress = new SocketAddress( "database.neo4j.com", 12345 );
        db = startDb( advertisedBoltAddress );

        RoutingResult expectedResult = newRoutingResult( advertisedBoltAddress );

        assertRoutingProceduresAvailable( databaseName, db, expectedResult );
    }

    @Test
    void shouldCallRoutingProcedureWithInvalidDatabaseName()
    {
        String unknownDatabaseName = "non-existing-database";
        db = startDb();

        assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, db );
    }

    protected DatabaseManagementServiceBuilder newGraphDatabaseFactory( Path homeDir )
    {
        return new TestDatabaseManagementServiceBuilder( homeDir );
    }

    private String boltAddress()
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( BoltConnector.NAME );
        assertNotNull( address );
        return address.toString();
    }

    private GraphDatabaseAPI startDb()
    {
        return startDb( null );
    }

    private GraphDatabaseAPI startDb( SocketAddress advertisedBoltAddress )
    {
        return (GraphDatabaseAPI) startDbms( advertisedBoltAddress ).database( DEFAULT_DATABASE_NAME );
    }

    DatabaseManagementService startDbms( SocketAddress advertisedBoltAddress )
    {
        DatabaseManagementServiceBuilder builder = newGraphDatabaseFactory( testDirectory.homePath() );
        builder.setConfig( auth_enabled, false );
        builder.setConfig( BoltConnector.enabled, true );
        builder.setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
        if ( advertisedBoltAddress != null )
        {
            builder.setConfig( BoltConnector.advertised_address, advertisedBoltAddress );
        }
        else
        {
            builder.setConfig( BoltConnector.advertised_address, new SocketAddress( 0 ) );
        }
        managementService = builder.build();
        return managementService;
    }

    static RoutingResult newRoutingResult( SocketAddress advertisedBoltAddress )
    {
        List<SocketAddress> addresses = singletonList( advertisedBoltAddress );
        Duration ttl = Config.defaults().get( routing_ttl );
        return new RoutingResult( addresses, addresses, addresses, ttl.getSeconds() );
    }
}
