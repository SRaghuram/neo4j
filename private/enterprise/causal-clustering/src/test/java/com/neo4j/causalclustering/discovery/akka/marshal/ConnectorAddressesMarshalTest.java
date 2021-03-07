/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ConnectorAddressesMarshalTest extends BaseMarshalTest<ConnectorAddresses>
{
    @Override
    public Collection<ConnectorAddresses> originals()
    {
        return asList(
                ConnectorAddresses.fromList( emptyList() ),
                ConnectorAddresses.fromList( singletonList(
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.bolt, new SocketAddress( "host", 27 ) ) ) ),
                ConnectorAddresses.fromList( asList(
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.bolt, new SocketAddress( "host1", 27 ) ),
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.https, new SocketAddress( "host3", 798 ) ),
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.http, new SocketAddress( "host2", 37 ) ) ) )
        );
    }

    @Override
    public ChannelMarshal<ConnectorAddresses> marshal()
    {
        return new ConnectorAddresses.Marshal();
    }
}
