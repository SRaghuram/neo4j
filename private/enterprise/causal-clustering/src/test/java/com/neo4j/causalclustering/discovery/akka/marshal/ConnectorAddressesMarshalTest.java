/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.marshal.ChannelMarshal;

public class ConnectorAddressesMarshalTest implements BaseMarshalTest<ConnectorAddresses>
{
    @Override
    public Collection<ConnectorAddresses> originals()
    {
        return List.of(
                ConnectorAddresses.fromList( List.of() ),
                ConnectorAddresses.fromList( List.of(
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.bolt, new SocketAddress( "host", 27 ) ) ) ),
                ConnectorAddresses.fromList( List.of(
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.bolt, new SocketAddress( "host1", 27 ) ),
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.https, new SocketAddress( "host3", 798 ) ),
                        new ConnectorAddresses.ConnectorUri( ConnectorAddresses.Scheme.http, new SocketAddress( "host2", 37 ) ) ) )
        );
    }

    @Override
    public ChannelMarshal<ConnectorAddresses> marshal()
    {
        return ConnectorAddresses.Marshal.INSTANCE;
    }
}
