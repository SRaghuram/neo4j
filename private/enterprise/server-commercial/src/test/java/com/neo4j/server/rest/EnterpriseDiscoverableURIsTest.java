/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class EnterpriseDiscoverableURIsTest
{
    @Test
    void shouldExposeBoltRoutingIfCore()
    {
        // Given
        Config config = Config.newBuilder()
                .set( CommercialEditionSettings.mode, CommercialEditionSettings.Mode.CORE )
                .set( BoltConnector.enabled, true )
                .build();

        // When
        Map<String,Object> asd = toMap(
                EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() ) );

        // Then
        assertThat(asd.get("bolt_routing"), equalTo( "bolt+routing://localhost:7687" ));
    }

    @Test
    void shouldGrabPortFromRegisterIfSetTo0()
    {
        // Given
        Config config = Config.newBuilder()
                .set( CommercialEditionSettings.mode, CommercialEditionSettings.Mode.CORE )
                .set( BoltConnector.enabled, true )
                .set( BoltConnector.listen_address, new SocketAddress( 0 ) )
                .build();
        ConnectorPortRegister ports = new ConnectorPortRegister();
        ports.register( BoltConnector.NAME, new InetSocketAddress( 1337 ) );

        // When
        Map<String,Object> asd = toMap(
                EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, ports ) );

        // Then
        assertThat(asd.get("bolt_routing"), equalTo( "bolt+routing://localhost:1337" ));
    }

    private Map<String,Object> toMap( DiscoverableURIs uris )
    {
        Map<String,Object> out = new HashMap<>();
        uris.forEach( ( k, v ) -> out.put( k, v.toASCIIString() ) );
        return out;
    }
}
