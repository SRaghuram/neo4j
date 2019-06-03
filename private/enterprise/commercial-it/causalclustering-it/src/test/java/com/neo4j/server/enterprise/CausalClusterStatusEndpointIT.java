/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4j;
import org.neo4j.test.extension.TestDirectoryClassExtension;

import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.availabilityStatuses;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.getLeader;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.getStatusRaw;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.getWritableEndpoint;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.startCluster;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointHelpers.writeSomeData;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.coreFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.healthFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.lastAppliedRaftIndexFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.leaderFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.memberIdFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.millisSinceLastLeaderMessageSanityCheck;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.participatingInRaftGroup;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.raftMessageThroughputPerSecondFieldIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.FieldMatchers.votingMemberSetIs;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allEndpointsFieldValues;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allReplicaFieldValues;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.allValuesEqual;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.asCollection;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.canVote;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.lastAppliedRaftIndex;
import static com.neo4j.server.enterprise.CausalClusterStatusEndpointMatchers.serverStatusEndpoint;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

class CausalClusterStatusEndpointIT
{
    @RegisterExtension
    static TestDirectoryClassExtension testDirectoryClassExtension = new TestDirectoryClassExtension();

    private static CausalClusterInProcessBuilder.CausalCluster CLUSTER;

    @BeforeAll
    static void setupClass() throws InterruptedException
    {
        CLUSTER = startCluster( testDirectoryClassExtension.getTestDirectory() );
    }

    @AfterAll
    static void shutdownClass() throws InterruptedException
    {
        if ( CLUSTER != null )
        {
            CLUSTER.shutdown();
        }
    }

    @Test
    void leaderIsWritable() throws InterruptedException
    {
        Neo4j leader = getLeader( CLUSTER );
        assertEventually( canVote( serverStatusEndpoint( leader ) ), equalTo( true ), 1, TimeUnit.MINUTES );

        String raw = getStatusRaw( getWritableEndpoint( leader.httpURI() ) );
        assertEquals( "true", raw );
    }

    @Test
    void booleanEndpointsAreReachable() throws InterruptedException
    {
        for ( Neo4j core : CLUSTER.getCoreNeo4j() )
        {
            assertEventually( canVote( serverStatusEndpoint( core ) ), equalTo( true ), 1, TimeUnit.MINUTES );

            List<Boolean> availability = Arrays.asList( availabilityStatuses( core.httpURI() ) );
            long trues = availability.stream().filter( i -> i ).count();
            long falses = availability.stream().filter( i -> !i ).count();
            assertEquals( 1, falses, availability.toString() );
            assertEquals( 2, trues, availability.toString() );
        }
    }

    @Test
    void statusEndpointIsReachableAndReadable() throws Exception
    {
        // given there is data
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, CausalClusterStatusEndpointMatchers::getNodeCount ), everyItem( greaterThan( 0L ) ), 3,
                TimeUnit.MINUTES );

        // then cores are valid
        for ( Neo4j core : CLUSTER.getCoreNeo4j() )
        {
            writeSomeData( CLUSTER );
            assertEventually( serverStatusEndpoint( core ), coreFieldIs( equalTo( true ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), memberIdFieldIs( not( emptyOrNullString() ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), healthFieldIs( equalTo( true ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), leaderFieldIs( not( emptyOrNullString() ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), votingMemberSetIs( hasSize( 3 ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), participatingInRaftGroup( true ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( core ), millisSinceLastLeaderMessageSanityCheck( true ), 1, TimeUnit.MINUTES );
        }

        // and replicas are valid
        for ( Neo4j replica : CLUSTER.getReplicaControls() )
        {
            writeSomeData( CLUSTER );
            assertEventually( serverStatusEndpoint( replica ), coreFieldIs( equalTo( false ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), lastAppliedRaftIndexFieldIs( greaterThan( 0L ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), memberIdFieldIs( not( emptyOrNullString() ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), healthFieldIs( equalTo( true ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), leaderFieldIs( not( emptyOrNullString() ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), votingMemberSetIs( hasSize( 3 ) ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), participatingInRaftGroup( false ), 1, TimeUnit.MINUTES );
            assertEventually( serverStatusEndpoint( replica ), millisSinceLastLeaderMessageSanityCheck( false ), 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void replicasContainTheSameRaftIndexAsCores() throws Exception
    {
        // given starting conditions
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, CausalClusterStatusEndpointMatchers::getNodeCount ), allValuesEqual(), 1, TimeUnit.MINUTES );
        long initialLastAppliedRaftIndex = lastAppliedRaftIndex( asCollection( serverStatusEndpoint( getLeader( CLUSTER ) ) ) ).get()
                .stream()
                .findFirst()
                .orElseThrow( () -> new RuntimeException( "List is empty" ) );
        assertThat( initialLastAppliedRaftIndex, greaterThan( 0L ) );

        // when more data is added
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, CausalClusterStatusEndpointMatchers::getNodeCount ), everyItem( greaterThan( 1L ) ), 1,
                TimeUnit.MINUTES );

        // then all status endpoints have a matching last appliedRaftIndex
        assertEventually( lastAppliedRaftIndex( allEndpointsFieldValues( CLUSTER ) ), allValuesEqual(), 1, TimeUnit.MINUTES );

        // and endpoint last applied raft index has incremented
        assertEventually( serverStatusEndpoint( getLeader( CLUSTER ) ), lastAppliedRaftIndexFieldIs( greaterThan( initialLastAppliedRaftIndex ) ), 1,
                TimeUnit.MINUTES );
    }

    @Test
    void participatingInRaftGroupFalseWhenNotInGroup() throws InterruptedException
    {
        Neo4j first = CLUSTER.getCoreNeo4j().get( 0 );
        try
        {
            List<InProcessNeo4j> switchedOff =
                    CLUSTER.getCoreNeo4j().stream().filter( e -> !first.equals( e ) ).peek( InProcessNeo4j::close ).collect( Collectors.toList() );
            assert switchedOff.size() > 0;
            assertEventually( canVote( serverStatusEndpoint( first ) ), equalTo( false ), 1, TimeUnit.MINUTES );
        }
        finally
        {
            CLUSTER.shutdown();
            CLUSTER = startCluster( testDirectoryClassExtension.getTestDirectory() );
        }
    }

    @Test
    void throughputIsPositive() throws InterruptedException
    {
        writeSomeData( CLUSTER );
        assertEventually( allEndpointsFieldValues( CLUSTER ), everyItem( raftMessageThroughputPerSecondFieldIs( greaterThan( 0.0 ) ) ), 1, TimeUnit.MINUTES );
        assertEventually( allEndpointsFieldValues( CLUSTER ), everyItem( raftMessageThroughputPerSecondFieldIs( equalTo( 0.0 ) ) ), 90,
                TimeUnit.SECONDS );
    }
}
