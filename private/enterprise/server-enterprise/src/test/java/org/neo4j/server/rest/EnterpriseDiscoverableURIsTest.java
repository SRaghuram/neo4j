/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class EnterpriseDiscoverableURIsTest
{
    @Test
    void shouldExposeBoltRoutingIfCore()
    {
        // Given
        BoltConnector bolt = new BoltConnector( "honestJakesBoltConnector" );
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.CORE.name() )
                .withSetting( bolt.enabled, "true" )
                .withSetting( bolt.type, BoltConnector.ConnectorType.BOLT.name() )
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
        BoltConnector bolt = new BoltConnector( "honestJakesBoltConnector" );
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.CORE.name() )
                .withSetting( bolt.enabled, "true" )
                .withSetting( bolt.type, BoltConnector.ConnectorType.BOLT.name() )
                .withSetting( bolt.listen_address, ":0" )
                .build();
        ConnectorPortRegister ports = new ConnectorPortRegister();
        ports.register( bolt.key(), new InetSocketAddress( 1337 ) );

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
