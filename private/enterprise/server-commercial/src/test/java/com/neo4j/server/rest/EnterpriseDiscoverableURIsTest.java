/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class EnterpriseDiscoverableURIsTest
{
    @Test
    void shouldExposeManagementApi()
    {
        // Given
        var config = Config.defaults();

        // When
        var asd = toMap( EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() ) );

        // Then
        assertThat( asd.get( "management" ), equalTo( "/db/manage/" ) );
    }

    @Test
    void shouldBeAbleToResetManagementApi()
    {
        // Given
        var config = Config.defaults( ServerSettings.management_api_path, URI.create( "/a/new/manage/api" ) );

        // When
        var asd = toMap( EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() ) );

        // Then
        assertThat( asd.get( "management" ), equalTo( "/a/new/manage/api/" ) );
    }

    private Map<String,Object> toMap( DiscoverableURIs uris )
    {
        Map<String,Object> out = new HashMap<>();
        uris.forEach( out::put );
        return out;
    }
}
