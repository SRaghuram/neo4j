/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import org.junit.Test;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.https;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ClientConnectorAddressesTest
{
    @Test
    public void shouldSerializeToString()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "host", 2 ) ),
                new ConnectorUri( https, new AdvertisedSocketAddress( "host", 3 ) ),
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "::1", 4 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "::", 5 ) ),
                new ConnectorUri( https, new AdvertisedSocketAddress( "fe80:1:2::3", 6 ) ) )
        );

        String expectedString = "bolt://host:1,http://host:2,https://host:3,bolt://[::1]:4,http://[::]:5,https://[fe80:1:2::3]:6";

        // when
        String connectorAddressesString = connectorAddresses.toString();

        // then
        assertEquals( expectedString, connectorAddressesString );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddressesString );

        // then
        assertEquals( connectorAddresses, out );
    }

    @Test
    public void shouldSerializeWithNoHttpsAddress()
    {
        // given
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( asList(
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "host", 1 ) ),
                new ConnectorUri( http, new AdvertisedSocketAddress( "host", 2 ) )
        ) );

        // when
        ClientConnectorAddresses out = ClientConnectorAddresses.fromString( connectorAddresses.toString() );

        // then
        assertEquals( connectorAddresses, out );
    }
}
