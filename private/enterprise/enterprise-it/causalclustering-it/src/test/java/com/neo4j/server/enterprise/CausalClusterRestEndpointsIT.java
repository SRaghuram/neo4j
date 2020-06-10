/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.extension.Inject;

import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.httpURI;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryAvailabilityEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryClusterEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryLegacyClusterEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryLegacyClusterStatusEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryReadOnlyEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryStatusEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryWritableEndpoint;
import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.writeSomeData;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.coreFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.discoveryHealthFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.healthFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.lastAppliedRaftIndexFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.leaderFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.memberIdFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.millisSinceLastLeaderMessageSanityCheck;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.participatingInRaftGroup;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.raftMessageThroughputPerSecondFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.votingMemberSetIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allReplicaFieldValues;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allStatusEndpointValues;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allValuesEqual;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.asCollection;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.canVote;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.lastAppliedRaftIndex;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.statusEndpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ClusterExtension
class CausalClusterRestEndpointsIT
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
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        for ( var core : cluster.coreMembers() )
        {
            assertEventually( canVote( statusEndpoint( core, KNOWN_DB ) ), TRUE, 1, MINUTES );
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

                var baseUri = httpURI( neo4j ).resolve( "/db/" + KNOWN_DB + "/cluster/" );

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

                var baseUri = httpURI( neo4j ).resolve( "/db/manage/server/causalclustering/" );

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
    void writableEndpointsAreReachable() throws TimeoutException
    {
        var leader = cluster.awaitLeader( KNOWN_DB );
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryWritableEndpoint( core, KNOWN_DB );

                if ( core == leader )
                {
                    assertEquals( OK.getStatusCode(), response.statusCode() );
                    assertTrue( response.body() );
                }
                else
                {
                    assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                    assertFalse( response.body() );
                }
            } );
        }

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
    void shouldRedirectWritableEndpoints() throws TimeoutException
    {
        var leader = cluster.awaitLeader( KNOWN_DB );
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( core, "writable" );

                if ( core == leader )
                {
                    assertEquals( OK.getStatusCode(), response.statusCode() );
                    assertTrue( response.body() );
                }
                else
                {
                    assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                    assertFalse( response.body() );
                }
            } );
        }

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
    void readOnlyEndpointsAreReachable() throws TimeoutException
    {
        var leader = cluster.awaitLeader( KNOWN_DB );
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryReadOnlyEndpoint( core, KNOWN_DB );

                if ( core == leader )
                {
                    assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                    assertFalse( response.body() );
                }
                else
                {
                    assertEquals( OK.getStatusCode(), response.statusCode() );
                    assertTrue( response.body() );
                }
            } );
        }

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
    void shouldRedirectReadOnlyEndpoints() throws TimeoutException
    {
        var leader = cluster.awaitLeader( KNOWN_DB );
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( core, "read-only" );

                if ( core == leader )
                {
                    assertEquals( NOT_FOUND.getStatusCode(), response.statusCode() );
                    assertFalse( response.body() );
                }
                else
                {
                    assertEquals( OK.getStatusCode(), response.statusCode() );
                    assertTrue( response.body() );
                }
            } );
        }

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

        // then cores are valid
        for ( var core : cluster.coreMembers() )
        {
            writeSomeData( cluster, KNOWN_DB );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( coreFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( discoveryHealthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ) ),
                    1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( votingMemberSetIs( hasSize( 3 ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( participatingInRaftGroup( true ) ), 1, MINUTES );
            assertEventually( statusEndpoint( core, KNOWN_DB ), new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( true ) ), 1, MINUTES );
        }

        // and replicas are valid
        for ( var replica : cluster.readReplicas() )
        {
            writeSomeData( cluster, KNOWN_DB );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( discoveryHealthFieldIs( equalTo( true ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ) ),
                    1, MINUTES );
            assertEventually( statusEndpoint( replica, KNOWN_DB ), new HamcrestCondition<>( votingMemberSetIs( hasSize( 3 ) ) ), 1, MINUTES );
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
    void replicasContainTheSameRaftIndexAsCores() throws Exception
    {
        // given starting conditions
        var leader = cluster.awaitLeader( KNOWN_DB );

        writeSomeData( cluster, KNOWN_DB );
        assertEventually( allReplicaFieldValues( cluster, DataCreator::countNodes ), allValuesEqual(), 1, MINUTES );
        var initialLastAppliedRaftIndex = lastAppliedRaftIndex( asCollection(
                statusEndpoint( leader, KNOWN_DB ) ) ).call()
                .stream()
                .findFirst()
                .orElseThrow( () -> new RuntimeException( "List is empty" ) );
        assertThat( initialLastAppliedRaftIndex, greaterThan( 0L ) );

        // when more data is added
        writeSomeData( cluster, KNOWN_DB );
        assertEventually( allReplicaFieldValues( cluster, DataCreator::countNodes ),
                new HamcrestCondition<>( everyItem( greaterThan( 1L ) ) ), 1, MINUTES );

        // then all status endpoints have a matching last appliedRaftIndex
        assertEventually( lastAppliedRaftIndex( allStatusEndpointValues( cluster, KNOWN_DB ) ), allValuesEqual(), 1, MINUTES );

        // and endpoint last applied raft index has incremented
        assertEventually( statusEndpoint( leader, KNOWN_DB ),
                new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( greaterThan( initialLastAppliedRaftIndex ) ) ),
                1, MINUTES );
    }

    @Test
    void throughputIsPositive() throws Exception
    {
        writeSomeData( cluster, KNOWN_DB );
        assertEventually( allStatusEndpointValues( cluster, KNOWN_DB ),
                new HamcrestCondition<>( everyItem( raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ) ) ), 1, MINUTES );
        assertEventually( allStatusEndpointValues( cluster, KNOWN_DB ),
                new HamcrestCondition<>( everyItem( raftMessageThroughputPerSecondFieldIs( equalTo( 0.0 ) ) ) ), 90, SECONDS );
    }
}
