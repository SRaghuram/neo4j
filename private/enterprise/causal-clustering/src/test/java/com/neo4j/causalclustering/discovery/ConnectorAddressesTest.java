/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.ConnectorAddresses.ConnectorUri;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.https;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        var publicUris = connectorAddresses.publicUriList().stream()
                                           .map( URI::toString )
                                           .collect( Collectors.toList() );
        assertThat( publicUris ).doesNotContain( intraClusterUri.toString() );
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

        // when
        ConnectorAddresses out = ConnectorAddresses.fromString( connectorAddressesString );

        // then
        assertEquals( connectorAddresses, out );
    }

    @Test
    void shouldSerializeWithNoHttpsAddress()
    {
        // given
        ConnectorAddresses connectorAddresses = ConnectorAddresses.fromList( asList(
                new ConnectorUri( bolt, new SocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new SocketAddress( "host", 2 ) )
        ) );

        // when
        ConnectorAddresses out = ConnectorAddresses.fromString( connectorAddresses.toString() );

        // then
        assertEquals( connectorAddresses, out );
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
}