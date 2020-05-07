/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.configuration.GraphDatabaseSettings.Mode.CORE;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.READ_REPLICA;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.SINGLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;

class EnterpriseDiscoverableURIsTest
{
    @Test
    void shouldNotExposeCausalClusteringManagementApiOnStandalone()
    {
        // Given
        var config = Config.defaults( GraphDatabaseSettings.mode, SINGLE );

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertNull( discoverableURIs.get( "cluster" ) );
    }

    @Test
    void shouldExposeCausalClusteringManagementApiOnCore()
    {
        // Given
        var config = Config.defaults( GraphDatabaseSettings.mode, CORE );

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "cluster" ), equalTo( "/db/{databaseName}/cluster" ) );
    }

    @Test
    void shouldExposeCausalClusteringManagementApiOnReadReplica()
    {
        // Given
        var config = Config.defaults( GraphDatabaseSettings.mode, READ_REPLICA );

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "cluster" ), equalTo( "/db/{databaseName}/cluster" ) );
    }

    @Test
    void shouldConfigureCausalClusteringManagementApiOnCore()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, CORE )
                .set( ServerSettings.db_api_path, URI.create( "/a/new/core/db/path" ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "cluster" ), equalTo( "/a/new/core/db/path/{databaseName}/cluster" ) );
    }

    @Test
    void shouldConfigureCausalClusteringManagementApiOnReadReplica()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, READ_REPLICA )
                .set( ServerSettings.db_api_path, URI.create( "/a/new/read_replica/db/path" ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "cluster" ), equalTo( "/a/new/read_replica/db/path/{databaseName}/cluster" ) );
    }

    private static Map<String,Object> findEnterpriseDiscoverableURIs( Config config )
    {
        var discoverableURIs = EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() );
        var result = new HashMap<String,Object>();
        discoverableURIs.forEach( result::put );
        return result;
    }
}
