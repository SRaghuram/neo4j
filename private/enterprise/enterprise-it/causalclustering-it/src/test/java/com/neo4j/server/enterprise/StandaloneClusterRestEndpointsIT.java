/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.clusterEndpointBase;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.legacyClusterEndpointBase;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryAvailabilityEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryClusterEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryLegacyClusterEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryLegacyClusterStatusEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryReadOnlyEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryStatusEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryWritableEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.writeSomeData;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.coreFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.discoveryHealthFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.healthFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.lastAppliedRaftIndexFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.leaderFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.memberIdFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.millisSinceLastLeaderMessageSanityCheck;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.participatingInRaftGroup;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.raftMessageThroughputPerSecondFieldIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.FieldMatchers.votingMemberSetIs;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.allReplicaFieldValues;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.combinedStatusEndpoint;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.databaseHealthy;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.statusEndpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ClusterExtension
class StandaloneClusterRestEndpointsIT
{
    private static final String KNOWN_DB = DEFAULT_DATABASE_NAME;
    private static final String UNKNOWN_DB = "foobar";

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void setupClass() throws ExecutionException, InterruptedException
    {
        var clusterConfig = ClusterConfig
                .clusterConfig()
                .withStandalone()
                .withNumberOfReadReplicas( 2 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        for ( var member : cluster.allMembers() )
        {
            assertEventually( databaseHealthy( statusEndpoint( member, KNOWN_DB ) ), TRUE, 1, MINUTES );
        }
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
    void clusterEndpointsAreReachable()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryClusterEndpoint( neo4j, KNOWN_DB );
                assertEquals( OK.getStatusCode(), response.statusCode() );

                var baseUri = clusterEndpointBase( neo4j, KNOWN_DB );

                var expectedBody = Map.<String,Object>of(
                        "available", baseUri.resolve( "available" ).toString(),
                        "writable", baseUri.resolve( "writable" ).toString(),
                        "read-only", baseUri.resolve( "read-only" ).toString(),
                        "status", baseUri.resolve( "status" ).toString() );

                assertEquals( expectedBody, response.body() );
            } );
        }
    }

    @Test
    void clusterEndpointsAreNotReachableForUnknownDatabase()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryClusterEndpoint( neo4j, UNKNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertThat( response.body(), is( not( anEmptyMap() ) ) );
            } );
        }
    }

    @Test
    void shouldRedirectClusterEndpoints()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( neo4j );
                assertEquals( OK.getStatusCode(), response.statusCode() );

                var baseUri = legacyClusterEndpointBase( neo4j );

                var expectedBody = Map.<String,Object>of(
                        "available", baseUri.resolve( "available" ).toString(),
                        "writable", baseUri.resolve( "writable" ).toString(),
                        "read-only", baseUri.resolve( "read-only" ).toString(),
                        "status", baseUri.resolve( "status" ).toString() );

                assertEquals( expectedBody, response.body() );
            } );
        }
    }

    @Test
    void availabilityEndpointsAreReachable()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryAvailabilityEndpoint( neo4j, KNOWN_DB );
                assertEquals( OK.getStatusCode(), response.statusCode() );
                assertTrue( response.body() );
            } );
        }
    }

    @Test
    void availabilityEndpointsAreNotReachableForUnknownDatabase()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryAvailabilityEndpoint( neo4j, UNKNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertFalse( response.body() );
            } );
        }
    }

    @Test
    void shouldRedirectAvailabilityEndpoints()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( neo4j, "available" );
                assertEquals( OK.getStatusCode(), response.statusCode() );
                assertTrue( response.body() );
            } );
        }
    }

    @Test
    void writableEndpointsAreReachable()
    {
        var standalone = cluster.getStandaloneMember();
        awaitUntilAsserted( () ->
        {
            var response = queryWritableEndpoint( standalone, KNOWN_DB );
            assertEquals( OK.getStatusCode(), response.statusCode() );
            assertTrue( response.body() );
        } );

        for ( var replica : cluster.readReplicas() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryWritableEndpoint( replica, KNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertFalse( response.body() );
            } );
        }
    }

    @Test
    void writableEndpointsAreNotReachableForUnknownDatabase()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryWritableEndpoint( neo4j, UNKNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertFalse( response.body() );
            } );
        }
    }

    @Test
    void shouldRedirectWritableEndpoints()
    {
        var standalone = cluster.getStandaloneMember();
        awaitUntilAsserted( () ->
        {
            var response = queryLegacyClusterEndpoint( standalone, "writable" );
            assertEquals( OK.getStatusCode(), response.statusCode() );
            assertTrue( response.body() );
        } );

        for ( var replica : cluster.readReplicas() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( replica, "writable" );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertFalse( response.body() );
            } );
        }
    }

    @Test
    void readOnlyEndpointsAreReachable()
    {
        var standalone = cluster.getStandaloneMember();
        awaitUntilAsserted( () ->
        {
            var response = queryReadOnlyEndpoint( standalone, KNOWN_DB );
            assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
            assertFalse( response.body() );
        } );

        for ( var replica : cluster.readReplicas() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryReadOnlyEndpoint( replica, KNOWN_DB );
                assertEquals( OK.getStatusCode(), response.statusCode() );
                assertTrue( response.body() );
            } );
        }
    }

    @Test
    void readOnlyEndpointsAreNotReachableForUnknownDatabase()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryReadOnlyEndpoint( neo4j, UNKNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertFalse( response.body() );
            } );
        }
    }

    @Test
    void shouldRedirectReadOnlyEndpoints()
    {

        var standalone = cluster.getStandaloneMember();
        awaitUntilAsserted( () ->
        {
            var response = queryLegacyClusterEndpoint( standalone, "read-only" );
            assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
            assertFalse( response.body() );
        } );

        for ( var replica : cluster.readReplicas() )
        {
            var response = queryLegacyClusterEndpoint( replica, "read-only" );
            assertEquals( OK.getStatusCode(), response.statusCode() );
            assertTrue( response.body() );
        }
    }

    @Test
    void statusEndpointsAreNotReachableForUnknownDatabase()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryStatusEndpoint( neo4j, UNKNOWN_DB );
                assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                assertThat( response.body(), is( not( anEmptyMap() ) ) );
            } );
        }
    }

    @Test
    void statusEndpointIsReachableAndReadable() throws Exception
    {
        // given there is data
        writeSomeData( cluster, KNOWN_DB );
        assertEventually( allReplicaFieldValues( cluster, DataCreator::countNodes ),
                new HamcrestCondition<>( everyItem( greaterThan( 0L ) ) ), 3, MINUTES );

        // then standalone is valid
        var standalone = cluster.getStandaloneMember();

        writeSomeData( cluster, KNOWN_DB );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( equalTo( 0L ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( discoveryHealthFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ),
                new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( nullValue( Double.class ) ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( votingMemberSetIs( empty() ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( participatingInRaftGroup( false ) ), 1, MINUTES );
        assertEventually( statusEndpoint( standalone, KNOWN_DB ), new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( false ) ), 1, MINUTES );

        // and replicas are valid
        for ( var replica : cluster.readReplicas() )
        {
            writeSomeData( cluster, KNOWN_DB );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( equalTo( 0L ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( discoveryHealthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ),
                    new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( equalTo( 0.0 ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( votingMemberSetIs( empty() ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( participatingInRaftGroup( false ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( false ) ), 1, MINUTES );
        }
    }

    @Test
    void shouldRedirectStatusEndpoints()
    {
        for ( var neo4j : cluster.allMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterStatusEndpoint( neo4j );
                assertEquals( OK.getStatusCode(), response.statusCode() );
            } );
        }
    }

    @Test
    void standaloneAndReadReplicasHaveCombinedStatusEndpoint() throws Exception
    {
        writeSomeData( cluster, DEFAULT_DATABASE_NAME );
        assertEventually( allReplicaFieldValues( cluster, DataCreator::countNodes ),
                new HamcrestCondition<>( everyItem( greaterThan( 0L ) ) ), 3, MINUTES );

        var newDatabaseName = "foo";
        createDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyStarted( newDatabaseName, cluster );

        var systemDbUuid = databaseUuid( SYSTEM_DATABASE_NAME );
        var defaultDbUuid = databaseUuid( DEFAULT_DATABASE_NAME );
        var newDbUuid = databaseUuid( newDatabaseName );

        var standalone = cluster.getStandaloneMember();
        writeSomeData( cluster, DEFAULT_DATABASE_NAME );
        writeSomeData( cluster, newDatabaseName );

        assertEventually( combinedStatusEndpoint( standalone ), statuses -> statuses.size() == 3, 1, MINUTES );
        verifyCombinedStatusEndpointOnStandalone( standalone, SYSTEM_DATABASE_NAME, systemDbUuid );
        verifyCombinedStatusEndpointOnStandalone( standalone, DEFAULT_DATABASE_NAME, defaultDbUuid );
        verifyCombinedStatusEndpointOnStandalone( standalone, newDatabaseName, newDbUuid );

        for ( var readReplica : cluster.readReplicas() )
        {
            writeSomeData( cluster, DEFAULT_DATABASE_NAME );
            writeSomeData( cluster, newDatabaseName );

            assertEventually( combinedStatusEndpoint( readReplica ), statuses -> statuses.size() == 3, 1, MINUTES );
            verifyCombinedStatusEndpointOnReadReplica( readReplica, SYSTEM_DATABASE_NAME, systemDbUuid );
            verifyCombinedStatusEndpointOnReadReplica( readReplica, DEFAULT_DATABASE_NAME, defaultDbUuid );
            verifyCombinedStatusEndpointOnReadReplica( readReplica, newDatabaseName, newDbUuid );
        }

        dropDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyDoesNotExist( newDatabaseName, cluster );
    }

    private static void verifyCombinedStatusEndpointOnStandalone( ClusterMember standalone, String databaseName, UUID databaseUuid )
    {
        var standaloneStatus = ClusteringStatusEndpointMatchers.statusFromCombinedEndpoint( standalone, databaseName, databaseUuid );
        assertEventually( standaloneStatus, new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( equalTo( 0L ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( nullValue( Double.class ) ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( votingMemberSetIs( empty() ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( participatingInRaftGroup( false ) ), 1, MINUTES );
        assertEventually( standaloneStatus, new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( false ) ), 1, MINUTES );
    }

    private static void verifyCombinedStatusEndpointOnReadReplica( ClusterMember readReplica, String databaseName, UUID databaseUuid )
    {
        var readReplicaStatus = ClusteringStatusEndpointMatchers.statusFromCombinedEndpoint( readReplica, databaseName, databaseUuid );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( equalTo( 0L ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( greaterThanOrEqualTo( 0.0 ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( votingMemberSetIs( empty() ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( participatingInRaftGroup( false ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( false ) ), 1, MINUTES );
    }

    private UUID databaseUuid( String databaseName ) throws TimeoutException
    {
        var managementService = cluster.awaitLeader( databaseName ).managementService();
        var db = (GraphDatabaseAPI) managementService.database( databaseName );
        return db.databaseId().databaseId().uuid();
    }
}
