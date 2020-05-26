/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.ResultSummaryTestUtils.plan;
import static com.neo4j.ResultSummaryTestUtils.stats;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.connectors.BoltConnector.connector_routing_enabled;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( FabricEverywhereExtension.class )
@ExtendWith( SuppressOutputExtension.class )
@ExtendWith( MockedRoutingContextExtension.class )
@ClusterExtension
class CypherInClusterTest extends ClusterTestSupport
{
    @Inject
    protected static RoutingContext routingContext;

    @Inject
    private static ClusterFactory clusterFactory;

    protected static Cluster cluster;

    protected static Map<Integer,Driver> coreDrivers = new HashMap<>();

    protected static Driver readReplicaDriver;

    @BeforeAll
    static void beforeAll() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withSharedCoreParam( connector_routing_enabled, "true" )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedReadReplicaParam( connector_routing_enabled, "true" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        readReplicaDriver = getReadReplicaDriver( cluster );

        cluster.coreMembers().forEach( core -> coreDrivers.put( core.serverId(), driver( core.directURI() ) ) );

        var systemLeader = cluster.awaitLeader( "system" );
        Driver systemLeaderDriver = coreDrivers.get( systemLeader.serverId() );

        try ( var session = systemLeaderDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            session.run( "CREATE DATABASE foo" ).consume();
        }

        awaitDbAvailable( cluster, "foo" );
    }

    @BeforeEach
    void beforeEach() throws TimeoutException
    {
        super.beforeEach( routingContext, cluster, new ArrayList<>( coreDrivers.values() ), readReplicaDriver );
    }

    @AfterAll
    static void afterAll()
    {
        Stream.<Runnable>concat(
                coreDrivers.values().stream().map( driver -> driver::close ),
                Stream.of( cluster::shutdown, readReplicaDriver::close )
        ).parallel()
         .forEach( Runnable::run );
    }

    @Test
    void testWriteOnLeader()
    {
        doTestWriteOnClusterMember( fooLeaderDriver );
    }

    @Test
    void testWriteOnFollower()
    {
        doTestWriteOnClusterMember( fooFollowerDriver );
    }

    @Test
    void testWriteOnReadReplica()
    {
        doTestWriteOnClusterMember( readReplicaDriver );
    }

    @Test
    void testWriteOnFollowerWithRoutingDisabled()
    {
        when( routingContext.isServerRoutingEnabled() ).thenReturn( false );
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (:Person {name: 'Carrie',  uid: 2, age: 50})" );
        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> run( fooFollowerDriver, "neo4j", AccessMode.WRITE, session -> session.run( createQuery ).list() ) )
                .withMessage( "No write operations are allowed directly on this database." +
                        " Writes must pass through the leader." +
                        " The role of this server is: FOLLOWER" );
    }

    @Disabled
    @Test
    void testWriteWithNoLeader() throws Exception
    {
        var fooLeader = cluster.awaitLeader( "foo" );
        cluster.removeCoreMember( fooLeader );
        try
        {
            assertEquals( 1, cluster.numberOfCoreMembersReportedByTopology( "foo" ) );
            doWriteWithNoLeader( fooFollowerDriver );
            doWriteWithNoLeader( readReplicaDriver );
        }
        finally
        {
            cluster.newCoreMember().start();
            cluster.awaitLeader( "foo" );
        }
    }

    @Test
    void testResultSummaryCountersWithRouting()
    {
        var query = joinAsLines(
                "USE foo",
                "CREATE (n:Person {name: 'John Doe'})",
                "SET n:Friend"
        );

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE, session -> session.run( query ).consume() );

        var counters = resultSummary.counters();
        assertNotNull( counters );
        assertTrue( counters.containsUpdates() );
        assertFalse( counters.containsSystemUpdates() );
        assertEquals( 1, counters.nodesCreated() );
        assertEquals( 0, counters.nodesDeleted() );
        assertEquals( 0, counters.relationshipsCreated() );
        assertEquals( 0, counters.relationshipsDeleted() );
        assertEquals( 1, counters.propertiesSet() );
        assertEquals( 2, counters.labelsAdded() );
        assertEquals( 0, counters.labelsRemoved() );
        assertEquals( 0, counters.indexesAdded() );
        assertEquals( 0, counters.indexesRemoved() );
        assertEquals( 0, counters.constraintsAdded() );
        assertEquals( 0, counters.constraintsRemoved() );
        assertEquals( 0, counters.systemUpdates() );
    }

    @Test
    void testProfileRoutedQuery()
    {
        var query = joinAsLines(
                "PROFILE USE foo",
                "CREATE (n:Person {name: 'John Doe'})",
                "SET n:Friend",
                "RETURN n"
        );

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE, session -> session.run( query ).consume() );

        assertTrue( resultSummary.hasProfile() );
        var profiledPlan = resultSummary.profile();

        var expectedPlan =
                plan( "RemoteExecution@graph(0)",
                        plan( "ProduceResults@foo",
                                plan( "SetLabels@foo",
                                        plan( "Create@foo" )
                                )
                        )
                );

        expectedPlan.assertPlan( profiledPlan );

        var expectedStats =
                stats( 1, false,
                        stats( 1, false,
                                stats( 1, true,
                                        stats( 1, true )
                                )
                        )
                );

        expectedStats.assertStats( profiledPlan );
    }

    @Test
    void testExplainRoutedQuery()
    {
        var query = joinAsLines(
                "EXPLAIN USE foo",
                "CREATE (n:Person {name: 'John Doe'})",
                "SET n:Friend",
                "RETURN n"
        );

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE, session -> session.run( query ).consume() );

        assertTrue( resultSummary.hasPlan() );
        assertFalse( resultSummary.hasProfile() );
        var profiledPlan = resultSummary.plan();

        var expectedPlan =
                plan( "ProduceResults@foo",
                        plan( "SetLabels@foo",
                                plan( "Create@foo" )
                        )
                );

        expectedPlan.assertPlan( profiledPlan );
    }

    private void doWriteWithNoLeader( Driver driver )
    {
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (:Person {name: 'Carrie',  uid: 2, age: 50})" );
        assertThatExceptionOfType( TransientException.class )
                .isThrownBy( () -> run( driver, "neo4j", AccessMode.WRITE, session -> session.run( createQuery ).list() ) )
                .withMessage( "Unable to get bolt address of LEADER for database foo" );
    }
}
