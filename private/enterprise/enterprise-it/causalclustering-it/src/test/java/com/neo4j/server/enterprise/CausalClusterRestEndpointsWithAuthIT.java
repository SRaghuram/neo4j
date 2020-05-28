/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.http.HttpResponse;
import java.util.Map;
import java.util.function.BiFunction;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.Neo4j;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.awaitLeader;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryLegacyClusterEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryLegacyClusterStatusEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.startCluster;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestInstance( PER_CLASS )
@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class CausalClusterRestEndpointsWithAuthIT
{
    @Inject
    private static TestDirectory testDirectory;

    private static CausalClusterInProcessBuilder.CausalCluster cluster;

    @BeforeAll
    static void setupClass() throws Exception
    {
        Map<Setting<?>,?> additionalConf = Map.of(
                GraphDatabaseSettings.auth_enabled, true,
                CausalClusteringSettings.status_auth_enabled, false );

        cluster = startCluster( testDirectory, additionalConf );
        assertNotNull( awaitLeader( cluster, SYSTEM_DATABASE_NAME ) );
        assertNotNull( awaitLeader( cluster, DEFAULT_DATABASE_NAME ) );
    }

    @AfterAll
    static void shutdownClass()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldAllowToAccessDiscoveryEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( CausalClusterRestEndpointHelpers::queryClusterEndpoint );
    }

    @Test
    void shouldAllowToAccessWritableEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( CausalClusterRestEndpointHelpers::queryWritableEndpoint );
    }

    @Test
    void shouldAllowToAccessReadOnlyEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( CausalClusterRestEndpointHelpers::queryReadOnlyEndpoint );
    }

    @Test
    void shouldAllowToAccessAvailableEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( CausalClusterRestEndpointHelpers::queryAvailabilityEndpoint );
    }

    @Test
    void shouldAllowToAccessStatusEndpointUnauthenticated() throws Exception
    {
        verifyEndpointAccessible( CausalClusterRestEndpointHelpers::queryStatusEndpoint );
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

    private static void verifyEndpointAccessible( BiFunction<Neo4j,String,HttpResponse<?>> requestExecutor ) throws Exception
    {
        // HTTP response status code should be either 200 or 404. Some endpoints return 404 to indicate a negative response
        // The response status code should never be 401 Unauthorized.
        var endpointAccessible = new HamcrestCondition<>( oneOf( OK.getStatusCode(), NOT_FOUND.getStatusCode() ) );

        for ( var core : cluster.getCoresAndReadReplicas() )
        {
            assertEventually( () -> requestExecutor.apply( core, SYSTEM_DATABASE_NAME ).statusCode(), endpointAccessible, 1, MINUTES );
            assertEventually( () -> requestExecutor.apply( core, DEFAULT_DATABASE_NAME ).statusCode(), endpointAccessible, 1, MINUTES );
        }
    }
}
