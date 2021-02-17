/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.ConnectorAddresses.ConnectorUri;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.https;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;

class ConnectorAddressesTest
{
    @Test
    void shouldExcludeIntraClusterAddressFromPublicUris()
    {
        // given
        var connectorAddresses = ConnectorAddresses.fromList( asList(
                new ConnectorUri( bolt, new SocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new SocketAddress( "host", 2 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( bolt, new SocketAddress( "::1", 4 ) ),
                new ConnectorUri( http, new SocketAddress( "::", 5 ) ),
                new ConnectorUri( https, new SocketAddress( "fe80:1:2::3", 6 ) ) )
        );

        // when / then
        var expectedIntraClusterSocketAddress = new SocketAddress( "::1", 4 );
        assertEquals( Optional.of( expectedIntraClusterSocketAddress ), connectorAddresses.intraClusterBoltAddress() );

        // when / then
        var intraClusterUri = new ConnectorUri( bolt, new SocketAddress( "::1", 4 ) );
        assertThat( connectorAddresses.publicUriList() ).doesNotContain( intraClusterUri.toString() );
    }

    @Test
    void shouldReturnFirstBoltAddressAsClientBoltAddress()
    {
        // given
        var clientBoltAddress = new SocketAddress( "host", 1 );
        var intraClusterBoltAddress = new SocketAddress( "host", 2 );
        var connectorAddresses = ConnectorAddresses.fromList( List.of(
                new ConnectorUri( bolt, clientBoltAddress ),
                new ConnectorUri( bolt, intraClusterBoltAddress ),
                new ConnectorUri( http, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 4 ) )
        ) );

        // then
        assertEquals( connectorAddresses.clientBoltAddress(), clientBoltAddress );
    }

    @Test
    void shouldReturnEmptyIntraClusterBoltAddressGivenSingle()
    {
        // given
        var clientBoltAddress = new SocketAddress( "host", 1 );
        var connectorAddresses = ConnectorAddresses.fromList( List.of(
                new ConnectorUri( bolt, clientBoltAddress ),
                new ConnectorUri( http, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 4 ) )
        ) );

        // then
        assertEquals( connectorAddresses.intraClusterBoltAddress(), Optional.empty() );
    }

    @Test
    void shouldReturnSecondBoltAddressAsIntraClusterBoltAddress()
    {
        // given
        var clientBoltAddress = new SocketAddress( "host", 1 );
        var intraClusterBoltAddress = new SocketAddress( "host", 2 );
        var connectorAddresses = ConnectorAddresses.fromList( List.of(
                new ConnectorUri( bolt, clientBoltAddress ),
                new ConnectorUri( bolt, intraClusterBoltAddress ),
                new ConnectorUri( http, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 4 ) )
        ) );

        // then
        assertEquals( connectorAddresses.intraClusterBoltAddress(), Optional.of( intraClusterBoltAddress ) );
    }

    @Test
    void shouldSerializeToString()
    {
        // given
        ConnectorAddresses connectorAddresses = ConnectorAddresses.fromList( asList(
                new ConnectorUri( bolt, new SocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new SocketAddress( "host", 2 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( bolt, new SocketAddress( "::1", 4 ) ),
                new ConnectorUri( http, new SocketAddress( "::", 5 ) ),
                new ConnectorUri( https, new SocketAddress( "fe80:1:2::3", 6 ) ) )
        );

        String expectedString = "bolt://host:1,bolt://[::1]:4,http://host:2,https://host:3,http://[::]:5,https://[fe80:1:2::3]:6";

        // when
        String connectorAddressesString = connectorAddresses.toString();

        // then
        assertEquals( expectedString, connectorAddressesString );
    }

    @Test
    void shouldNotReturnIntraClusterBoltAddressForReadReplicaWhenBuiltFromConfig()
    {
        // given
        Config config = mock( Config.class );
        given( config.get( BoltConnector.advertised_address ) ).willReturn( new SocketAddress( "stub" ) );
        given( config.get( GraphDatabaseSettings.mode ) ).willReturn( GraphDatabaseSettings.Mode.READ_REPLICA );
        given( config.get( GraphDatabaseSettings.routing_advertised_address ) ).willReturn( new SocketAddress( "stub" ) );
        given( config.get( GraphDatabaseSettings.routing_enabled ) ).willReturn( true );
        given( config.get( HttpConnector.enabled ) ).willReturn( false );
        given( config.get( HttpsConnector.enabled ) ).willReturn( false );
        ConnectorAddresses connectorAddresses = ConnectorAddresses.fromConfig( config );

        // when
        Optional<SocketAddress> addrOpt = connectorAddresses.intraClusterBoltAddress();

        // then
        assertTrue( addrOpt.isEmpty() );
    }
}
