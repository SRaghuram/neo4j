/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.test.routing.ResultSummaryTestUtils.plan;
import static com.neo4j.test.routing.ResultSummaryTestUtils.stats;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ExtendWith( MockedRoutingContextExtension.class )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
class ClusterRoutingIT extends ClusterTestSupport
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
                                                   .withSharedCoreParam( GraphDatabaseSettings.routing_enabled, "true" )
                                                   .withSharedCoreParam( GraphDatabaseSettings.log_queries_obfuscate_literals, "true" )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedReadReplicaParam( GraphDatabaseSettings.routing_enabled, "true" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        readReplicaDriver = getReadReplicaDriver( cluster );

        cluster.coreMembers().forEach( core -> coreDrivers.put( core.index(), driver( core.directURI() ) ) );

        var systemLeader = cluster.awaitLeader( "system" );
        Driver systemLeaderDriver = coreDrivers.get( systemLeader.index() );

        try ( var session = systemLeaderDriver.session( SessionConfig.builder().withDatabase( "system" ).build() ) )
        {
            session.writeTransaction( tx -> tx.run( "CREATE DATABASE foo" ).consume() );
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
    void testAdminCommandOnFollower()
    {
        // admin command --> neo4j@follower(system) --> system@leader(system)
        when( routingContext.isServerRoutingEnabled() ).thenReturn( true );
        try
        {
            run( systemFollowerDriver, "neo4j", AccessMode.WRITE,
                 session -> session.run( "CREATE DATABASE dummy WAIT" ).consume() );
            var dbs = run( systemFollowerDriver, "neo4j", AccessMode.WRITE,
                           session -> session.run( "SHOW DATABASE dummy" ).stream()
                                             .map( r -> r.get( "name" ).asString() )
                                             .collect( Collectors.toSet() ) );

            assertThat( dbs ).containsExactly( "dummy" );
        }
        finally
        {
            run( systemFollowerDriver, "neo4j", AccessMode.WRITE,
                 session -> session.run( "DROP DATABASE dummy IF EXISTS WAIT" ).consume() );
        }
    }

    @Test
    void testWriteOnFollowerWithRoutingDisallowed()
    {
        // update --> neo4j@follower(foo)

        // When disallowing routing (from the client e.g. using bolt://) we should expect to get basic write-on-follower errors,
        // same as if we did: update --> foo@follower(foo)
        when( routingContext.isServerRoutingEnabled() ).thenReturn( false );
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (:Person {name: 'Carrie',  uid: 2, age: 50})" );
        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> run( fooFollowerDriver, "neo4j", AccessMode.WRITE,
                                        session -> session.writeTransaction( tx -> tx.run( createQuery ).list() ) ) )
                .withMessage( "No write operations are allowed directly on this database." +
                        " Writes must pass through the leader." +
                        " The role of this server is: FOLLOWER" );
    }

    @Test
    void testAdminCommandOnSystemDbFollowerWithRoutingDisallowed()
    {
        // admin command --> neo4j@follower(system)

        // When disallowing routing (from the client e.g. using bolt://) we should expect to get basic write-on-follower errors
        // same as if we did: admin command --> system@follower(system)
        when( routingContext.isServerRoutingEnabled() ).thenReturn( false );
        var createQuery = joinAsLines(
                "CREATE DATABASE dummy" );
        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> run( systemFollowerDriver, "neo4j", AccessMode.WRITE,
                                        session -> session.writeTransaction( tx -> tx.run( createQuery ).list() ) ) )
                .withMessage( "Failed to create the specified database 'dummy': Administration commands must be executed on the LEADER server." );
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

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE,
                                 session -> session.writeTransaction( tx -> tx.run( query ).consume() ) );

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

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE,
                                 session -> session.writeTransaction( tx -> tx.run( query ).consume() ) );

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
                        stats( 1, true,
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

        var resultSummary = run( fooFollowerDriver, "neo4j", AccessMode.WRITE,
                                 session -> session.writeTransaction( tx -> tx.run( query ).consume() ) );

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
                .isThrownBy( () -> run( driver, "neo4j", AccessMode.WRITE,
                                        session -> session.writeTransaction( tx -> tx.run( createQuery ).list() ) ) )
                .withMessage( "Unable to get bolt address of LEADER for database foo" );
    }
}
