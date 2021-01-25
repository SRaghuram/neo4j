/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.function.BiFunction;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;

import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryLegacyClusterEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryLegacyClusterStatusEndpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.Matchers.oneOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class CausalClusterRestEndpointsWithAuthIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void setupClass() throws Exception
    {
        var clusterConfig = ClusterConfig
                .clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                .withSharedReadReplicaParam( GraphDatabaseSettings.auth_enabled, "true" )
                .withSharedCoreParam( CausalClusteringSettings.status_auth_enabled, "false" )
                .withSharedReadReplicaParam( CausalClusteringSettings.status_auth_enabled, "false" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        cluster.awaitLeader( DEFAULT_DATABASE_NAME );
    }

    @AfterAll
    void shutdownClass()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldAllowToAccessDiscoveryEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ClusteringEndpointHelpers::queryClusterEndpoint );
    }

    @Test
    void shouldAllowToAccessWritableEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ClusteringEndpointHelpers::queryWritableEndpoint );
    }

    @Test
    void shouldAllowToAccessReadOnlyEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ClusteringEndpointHelpers::queryReadOnlyEndpoint );
    }

    @Test
    void shouldAllowToAccessAvailableEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ClusteringEndpointHelpers::queryAvailabilityEndpoint );
    }

    @Test
    void shouldAllowToAccessStatusEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ClusteringEndpointHelpers::queryStatusEndpoint );
    }

    @Test
    void shouldAllowToAccessLegacyDiscoveryEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ( member, ignore ) -> queryLegacyClusterEndpoint( member ) );
    }

    @Test
    void shouldAllowToAccessLegacyStatusEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( ( member, ignore ) -> queryLegacyClusterStatusEndpoint( member ) );
    }

    private void verifyEndpointAccessible( BiFunction<ClusterMember,String,HttpResponse<?>> requestExecutor ) throws Exception
    {
        // HTTP response status code should be either 200 or 404. Some endpoints return 404 to indicate a negative response
        // The response status code should never be 401 Unauthorized.
        var endpointAccessible = new HamcrestCondition<>( oneOf( OK.getStatusCode(), NOT_FOUND.getStatusCode() ) );

        for ( var member : cluster.allMembers() )
        {
            assertEventually( () -> requestExecutor.apply( member, SYSTEM_DATABASE_NAME ).statusCode(), endpointAccessible, 1, MINUTES );
            assertEventually( () -> requestExecutor.apply( member, DEFAULT_DATABASE_NAME ).statusCode(), endpointAccessible, 1, MINUTES );
        }
    }
}
