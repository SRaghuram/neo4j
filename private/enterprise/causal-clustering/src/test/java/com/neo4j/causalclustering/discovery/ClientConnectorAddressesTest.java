/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.https;
import static java.util.Arrays.asList;

class ClientConnectorAddressesTest
{
    @Test
    void shouldSerializeToString()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new SocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new SocketAddress( "host", 2 ) ),
                new ConnectorUri( https, new SocketAddress( "host", 3 ) ),
                new ConnectorUri( bolt, new SocketAddress( "::1", 4 ) ),
                new ConnectorUri( http, new SocketAddress( "::", 5 ) ),
                new ConnectorUri( https, new SocketAddress( "fe80:1:2::3", 6 ) ) )
        );

        String expectedString = "bolt://host:1,http://host:2,https://host:3,bolt://[::1]:4,http://[::]:5,https://[fe80:1:2::3]:6";

        // when
        String connectorAddressesString = connectorAddresses.toString();

        // then
        Assertions.assertEquals( expectedString, connectorAddressesString );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddressesString );

        // then
        Assertions.assertEquals( connectorAddresses, out );
    }

    @Test
    void shouldSerializeWithNoHttpsAddress()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new SocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new SocketAddress( "host", 2 ) )
        ) );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddresses.toString() );

        // then
        Assertions.assertEquals( connectorAddresses, out );
    }
}
