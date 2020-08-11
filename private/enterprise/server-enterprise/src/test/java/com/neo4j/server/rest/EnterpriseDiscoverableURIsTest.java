/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.server.configuration.ServerSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.CORE;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.READ_REPLICA;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.SINGLE;

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
        assertNull( discoverableURIs.get( "db/cluster" ) );
        assertNull( discoverableURIs.get( "dbms/cluster" ) );
    }

    @Test
    void shouldExposeCausalClusteringManagementApiOnCore()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, CORE )
                .set( CausalClusteringSettings.initial_discovery_members, List.of( new SocketAddress( "localhost" ) ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "db/cluster" ), equalTo( "/db/{databaseName}/cluster" ) );
        assertThat( discoverableURIs.get( "dbms/cluster" ), equalTo( "/dbms/cluster" ) );
    }

    @Test
    void shouldExposeCausalClusteringManagementApiOnReadReplica()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, READ_REPLICA )
                .set( CausalClusteringSettings.initial_discovery_members, List.of( new SocketAddress( "localhost" ) ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "db/cluster" ), equalTo( "/db/{databaseName}/cluster" ) );
        assertThat( discoverableURIs.get( "dbms/cluster" ), equalTo( "/dbms/cluster" ) );
    }

    @Test
    void shouldConfigureCausalClusteringManagementApiOnCore()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, CORE )
                .set( CausalClusteringSettings.initial_discovery_members, List.of( new SocketAddress( "localhost" ) ) )
                .set( ServerSettings.db_api_path, URI.create( "/a/new/core/db/path" ) )
                .set( ServerSettings.dbms_api_path, URI.create( "/yet/another/new/path" ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "db/cluster" ), equalTo( "/a/new/core/db/path/{databaseName}/cluster" ) );
        assertThat( discoverableURIs.get( "dbms/cluster" ), equalTo( "/yet/another/new/path/cluster" ) );
    }

    @Test
    void shouldConfigureCausalClusteringManagementApiOnReadReplica()
    {
        // Given
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, READ_REPLICA )
                .set( CausalClusteringSettings.initial_discovery_members, List.of( new SocketAddress( "localhost" ) ) )
                .set( ServerSettings.db_api_path, URI.create( "/a/new/read_replica/db/path" ) )
                .set( ServerSettings.dbms_api_path, URI.create( "/another/read_replica/path" ) )
                .build();

        // When
        var discoverableURIs = findEnterpriseDiscoverableURIs( config );

        // Then
        assertThat( discoverableURIs.get( "db/cluster" ), equalTo( "/a/new/read_replica/db/path/{databaseName}/cluster" ) );
        assertThat( discoverableURIs.get( "dbms/cluster" ), equalTo( "/another/read_replica/path/cluster" ) );
    }

    private static Map<String,Object> findEnterpriseDiscoverableURIs( Config config )
    {
        var discoverableURIs = EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() );
        var result = new HashMap<String,Object>();
        discoverableURIs.forEach( result::put );
        return result;
    }
}
