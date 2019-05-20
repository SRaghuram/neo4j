/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.routing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.time.Duration;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.configuration.connectors.Connector.ConnectorType.BOLT;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class CommunitySingleInstanceRoutingProcedureIT extends BaseRoutingProcedureIT
{
    private static final String CONNECTOR_NAME = "my_bolt";

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldContainRoutingProcedure()
    {
        AdvertisedSocketAddress advertisedBoltAddress = new AdvertisedSocketAddress( "neo4j.com", 7687 );
        db = startDb( advertisedBoltAddress );

        RoutingResult expectedResult = newRoutingResult( advertisedBoltAddress );

        assertRoutingProceduresAvailable( db, expectedResult );
    }

    @Test
    void shouldAllowRoutingDriverToReadAndWrite()
    {
        db = startDb();

        assertPossibleToReadAndWriteUsingRoutingDriver( boltAddress() );
    }

    @Test
    void shouldCallRoutingProcedureWithValidDatabaseName()
    {
        String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
        AdvertisedSocketAddress advertisedBoltAddress = new AdvertisedSocketAddress( "database.neo4j.com", 12345 );
        db = startDb( advertisedBoltAddress );

        RoutingResult expectedResult = newRoutingResult( advertisedBoltAddress );

        assertRoutingProceduresAvailable( databaseName, db, expectedResult );
    }

    @Test
    void shouldCallRoutingProcedureWithInvalidDatabaseName()
    {
        String unknownDatabaseName = "non_existing_database";
        db = startDb();

        assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, db );
    }

    protected DatabaseManagementServiceBuilder newGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestDatabaseManagementServiceBuilder( databaseRootDir );
    }

    private String boltAddress()
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( CONNECTOR_NAME );
        assertNotNull( address );
        return address.toString();
    }

    private GraphDatabaseAPI startDb()
    {
        return startDb( null );
    }

    private GraphDatabaseAPI startDb( AdvertisedSocketAddress advertisedBoltAddress )
    {
        BoltConnector connector = new BoltConnector( CONNECTOR_NAME );

        DatabaseManagementServiceBuilder builder = newGraphDatabaseFactory( testDirectory.storeDir() );
        builder.setConfig( auth_enabled, FALSE );
        builder.setConfig( connector.enabled, TRUE );
        builder.setConfig( connector.type, BOLT.toString() );
        builder.setConfig( connector.encryption_level, OPTIONAL.toString() );
        builder.setConfig( connector.listen_address, "localhost:0" );
        if ( advertisedBoltAddress != null )
        {
            builder.setConfig( connector.advertised_address, advertisedBoltAddress.toString() );
        }
        managementService = builder.build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static RoutingResult newRoutingResult( AdvertisedSocketAddress advertisedBoltAddress )
    {
        List<AdvertisedSocketAddress> addresses = singletonList( advertisedBoltAddress );
        Duration ttl = Config.defaults().get( routing_ttl );
        return new RoutingResult( addresses, addresses, addresses, ttl.getSeconds() );
    }
}
