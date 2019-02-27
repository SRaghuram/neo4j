/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.routing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.configuration.connectors.Connector.ConnectorType.BOLT;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class CommunityRoutingProcedureIT extends BaseRoutingProcedureIT
{
    private static final String CONNECTOR_NAME = "my_bolt";

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    void shouldContainRoutingProcedure()
    {
        AdvertisedSocketAddress advertisedBoltAddress = new AdvertisedSocketAddress( "neo4j.com", 7687 );
        db = startCommunityDb( advertisedBoltAddress );

        List<AdvertisedSocketAddress> addresses = singletonList( advertisedBoltAddress );
        Duration ttl = Config.defaults().get( routing_ttl );
        RoutingResult expectedResult = new RoutingResult( addresses, addresses, addresses, ttl.getSeconds() );

        assertRoutingProceduresAvailable( db, expectedResult );
    }

    @Test
    void shouldAllowRoutingDriverToReadAndWrite()
    {
        db = startCommunityDb();

        assertPossibleToReadAndWriteUsingRoutingDriver( boltAddress() );
    }

    private String boltAddress()
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( CONNECTOR_NAME );
        assertNotNull( address );
        return address.toString();
    }

    private GraphDatabaseAPI startCommunityDb()
    {
        return startCommunityDb( null );
    }

    private GraphDatabaseAPI startCommunityDb( AdvertisedSocketAddress advertisedBoltAddress )
    {
        BoltConnector connector = new BoltConnector( CONNECTOR_NAME );

        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.storeDir() );
        builder.setConfig( auth_enabled, FALSE );
        builder.setConfig( connector.enabled, TRUE );
        builder.setConfig( connector.type, BOLT.toString() );
        builder.setConfig( connector.encryption_level, OPTIONAL.toString() );
        builder.setConfig( connector.listen_address, "localhost:0" );
        if ( advertisedBoltAddress != null )
        {
            builder.setConfig( connector.advertised_address, advertisedBoltAddress.toString() );
        }
        return (GraphDatabaseAPI) builder.newGraphDatabase();
    }
}
