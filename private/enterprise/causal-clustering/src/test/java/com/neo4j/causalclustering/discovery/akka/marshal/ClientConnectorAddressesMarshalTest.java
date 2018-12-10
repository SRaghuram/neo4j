/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith( Parameterized.class )
public class ClientConnectorAddressesMarshalTest extends BaseMarshalTest<ClientConnectorAddresses>
{
    public ClientConnectorAddressesMarshalTest( ClientConnectorAddresses original )
    {
        super( original, new ClientConnectorAddresses.Marshal() );
    }

    @Parameterized.Parameters ( name = "{0}" )
    public static Collection<ClientConnectorAddresses> data()
    {
        return asList(
                new ClientConnectorAddresses( emptyList() ),
                new ClientConnectorAddresses( singletonList(
                        new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt, new AdvertisedSocketAddress( "host", 27 ) ) ) ),
                new ClientConnectorAddresses( asList(
                        new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt, new AdvertisedSocketAddress( "host1", 27 ) ),
                        new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.https, new AdvertisedSocketAddress( "host3", 798 ) ),
                        new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.http, new AdvertisedSocketAddress( "host2", 37 ) ) ) )
        );

    }
}
