/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.functional;

import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.server.NeoServer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.mode;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class EnterpriseServerIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldBeAbleToStartInHAMode() throws Throwable
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = CommercialServerBuilder.serverOnRandomPorts()
                .usingDataDir( testDirectory.directory().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 200, r.getStatus() );
            assertThat( r.getEntity( String.class ), containsString( "master" ) );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    void shouldRequireAuthorizationForHAStatusEndpoints() throws Exception
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = CommercialServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), "true" )
                .usingDataDir( testDirectory.directory().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 401, r.getStatus() );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    void shouldAllowDisablingAuthorizationOnHAStatusEndpoints() throws Exception
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = CommercialServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), "true" )
                .withProperty( HaSettings.ha_status_auth_enabled.name(), "false" )
                .usingDataDir( testDirectory.directory().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 200, r.getStatus() );
            assertThat( r.getEntity( String.class ), containsString( "master" ) );
        }
        finally
        {
            server.stop();
        }
    }

    private static String getHaEndpoint( NeoServer server )
    {
        return server.baseUri().toString() + "db/manage/server/ha";
    }
}
