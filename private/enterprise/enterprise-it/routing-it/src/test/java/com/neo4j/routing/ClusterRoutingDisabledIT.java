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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ExtendWith( MockedRoutingContextExtension.class )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
class ClusterRoutingDisabledIT extends ClusterTestSupport
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
                                                   .withSharedCoreParam( GraphDatabaseSettings.routing_enabled, "false" )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedReadReplicaParam( GraphDatabaseSettings.routing_enabled, "false" );

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
    void testWriteOnFollowerWithRoutingDisabled()
    {
        // update --> neo4j@follower(foo) -X-> foo@leader(foo)

        // When we need routing, but SSR is disabled on the server, we should expect an error saying so
        when( routingContext.isServerRoutingEnabled() ).thenReturn( true );
        var createQuery = joinAsLines(
                "USE foo",
                "CREATE (:Person {name: 'Carrie',  uid: 2, age: 50})" );
        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> run( fooFollowerDriver, "neo4j", AccessMode.WRITE,
                                        session -> session.writeTransaction( tx -> tx.run( createQuery ).list() ) ) )
                .withMessage( "Unable to route write operation to leader for database 'foo'. Server-side routing is disabled.\n" +
                              "Either connect to the database directly using the driver (or interactively with the :use command),\n" +
                              "or enable server-side routing by setting `dbms.routing.enabled=true`" );
    }

    @Test
    void testAdminCommandOnSystemDbFollowerWithRoutingDisabled()
    {
        // admin command --> neo4j@follower(system) -X-> system@leader(system)

        // When we need routing, but SSR is disabled on the server, we should expect an error saying so
        when( routingContext.isServerRoutingEnabled() ).thenReturn( true );
        var createQuery = joinAsLines(
                "CREATE DATABASE dummy" );
        assertThatExceptionOfType( ClientException.class )
                .isThrownBy( () -> run( systemFollowerDriver, "neo4j", AccessMode.WRITE,
                                        session -> session.writeTransaction( tx -> tx.run( createQuery ).list() ) ) )
                .withMessage( "Unable to route write operation to leader for database 'system'. Server-side routing is disabled.\n" +
                              "Either connect to the database directly using the driver (or interactively with the :use command),\n" +
                              "or enable server-side routing by setting `dbms.routing.enabled=true`" );
    }
}
