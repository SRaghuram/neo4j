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
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.allStatusEndpointValues;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.allValuesEqual;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.asCollection;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.canVote;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.combinedStatusEndpoint;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.lastAppliedRaftIndex;
import static com.neo4j.server.enterprise.ClusteringStatusEndpointMatchers.statusEndpoint;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
    void writableEndpointsAreReachable() throws TimeoutException
    {
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryWritableEndpoint( core, KNOWN_DB );

                if ( cluster.isCoreLeader( core,  KNOWN_DB ) )
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
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( core, "writable" );

                if ( cluster.isCoreLeader( core,  KNOWN_DB )  )
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
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryReadOnlyEndpoint( core, KNOWN_DB );

                if ( cluster.isCoreLeader( core,  KNOWN_DB )  )
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
        for ( var core : cluster.coreMembers() )
        {
            awaitUntilAsserted( () ->
            {
                var response = queryLegacyClusterEndpoint( core, "read-only" );

                if ( cluster.isCoreLeader( core,  KNOWN_DB )  )
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
    void replicasContainTheSameRaftIndexAsCores() throws Exception
    {
        // given starting conditions
        writeSomeData( cluster, KNOWN_DB );
        assertEventually( allReplicaFieldValues( cluster, DataCreator::countNodes ), allValuesEqual(), 1, MINUTES );
        var initialLastAppliedRaftIndex = lastAppliedRaftIndex( asCollection(
                statusEndpoint( cluster.awaitLeader( KNOWN_DB ), KNOWN_DB ) ) ).call()
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
        assertEventually( statusEndpoint( cluster.awaitLeader( KNOWN_DB ), KNOWN_DB ),
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

    @Test
    void coresAndReadReplicasHaveCombinedStatusEndpoint() throws Exception
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

        for ( var core : cluster.coreMembers() )
        {
            writeSomeData( cluster, DEFAULT_DATABASE_NAME );
            writeSomeData( cluster, newDatabaseName );

            assertEventually( combinedStatusEndpoint( core ), statuses -> statuses.size() == 3, 1, MINUTES );
            verifyCombinedStatusEndpointOnCore( core, SYSTEM_DATABASE_NAME, systemDbUuid );
            verifyCombinedStatusEndpointOnCore( core, DEFAULT_DATABASE_NAME, defaultDbUuid );
            verifyCombinedStatusEndpointOnCore( core, newDatabaseName, newDbUuid );
        }

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

    @Test
    void newReplicaShouldHaveCorrectCombinedStatusEndpoint() throws Exception
    {
        var newDatabaseName = "foo";
        createDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyStarted( newDatabaseName, cluster );

        var systemDbUuid = databaseUuid( SYSTEM_DATABASE_NAME );
        var defaultDbUuid = databaseUuid( DEFAULT_DATABASE_NAME );
        var newDbUuid = databaseUuid( newDatabaseName );

        writeSomeData( cluster, DEFAULT_DATABASE_NAME );
        writeSomeData( cluster, newDatabaseName );

        var newReplica = cluster.addReadReplicaWithIndex( 9 );
        newReplica.start();

        assertEventually( combinedStatusEndpoint( newReplica ), statuses -> statuses.size() == 3, 1, MINUTES );
        verifyCombinedStatusEndpointOnReadReplica( newReplica, SYSTEM_DATABASE_NAME, systemDbUuid );
        verifyCombinedStatusEndpointOnReadReplica( newReplica, DEFAULT_DATABASE_NAME, defaultDbUuid );
        verifyCombinedStatusEndpointOnReadReplica( newReplica, newDatabaseName, newDbUuid );

        cluster.removeReadReplica( newReplica );

        dropDatabase( newDatabaseName, cluster );
        assertDatabaseEventuallyDoesNotExist( newDatabaseName, cluster );
    }

    private static void verifyCombinedStatusEndpointOnCore( ClusterMember core, String databaseName, UUID databaseUuid ) throws Exception
    {
        var coreStatus = ClusteringStatusEndpointMatchers.statusFromCombinedEndpoint( core, databaseName, databaseUuid );
        assertEventually( coreStatus, new HamcrestCondition<>( coreFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( memberIdFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( healthFieldIs( equalTo( true ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( leaderFieldIs( not( emptyOrNullString() ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( raftMessageThroughputPerSecondFieldIs( greaterThanOrEqualTo( 0.0 ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( votingMemberSetIs( hasSize( 3 ) ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( participatingInRaftGroup( true ) ), 1, MINUTES );
        assertEventually( coreStatus, new HamcrestCondition<>( millisSinceLastLeaderMessageSanityCheck( true ) ), 1, MINUTES );
    }

    private static void verifyCombinedStatusEndpointOnReadReplica( ClusterMember readReplica, String databaseName, UUID databaseUuid ) throws Exception
    {
        var readReplicaStatus = ClusteringStatusEndpointMatchers.statusFromCombinedEndpoint( readReplica, databaseName, databaseUuid );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( coreFieldIs( equalTo( false ) ) ), 1, MINUTES );
        assertEventually( readReplicaStatus, new HamcrestCondition<>( lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ) ), 1, MINUTES );
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
