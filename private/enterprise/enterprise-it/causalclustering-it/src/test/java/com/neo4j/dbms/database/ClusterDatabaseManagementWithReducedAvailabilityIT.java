/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class ClusterDatabaseManagementWithReducedAvailabilityIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster startCluster( int initialCores, int initialReplicas ) throws InterruptedException, ExecutionException
    {
        ClusterConfig clusterConfig = ClusterConfig
                .clusterConfig()
                .withSharedCoreParam( BoltConnector.enabled, "false" )
                .withSharedReadReplicaParam( BoltConnector.enabled, "false" )
                .withNumberOfCoreMembers( initialCores )
                .withNumberOfReadReplicas( initialReplicas );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }

    private static Stream<Arguments> clusterConfig()
    {
        return Stream.of(
                Arguments.of( 2, 0, 0, 0 ),
                Arguments.of( 3, 1, 1, 0 ),
                Arguments.of( 4, 1, 1, 1 ),
                Arguments.of( 5, 1, 2, 1 ),
                Arguments.of( 5, 2, 2, 2 )
                // note that unavailability higher than a minority would fail the management command itself
        );
    }

    @ParameterizedTest( name = "initialCores={0} coresToShutdown={1} initialReplicas={2} replicasToShutdown={3}" )
    @MethodSource( "clusterConfig" )
    void shouldCreateDatabaseWithReducedAvailability( int initialCores, int coresToShutdown, int initialReplicas, int replicasToShutdown ) throws Exception
    {
        // given: a cluster with a few dead members
        Cluster cluster = startCluster( initialCores, initialReplicas );

        Set<ClusterMember> deadMembers = shutdownMembers( cluster, coresToShutdown, replicasToShutdown );
        Set<ClusterMember> aliveMembers = aliveMembers( cluster, deadMembers );

        // when: creating a database
        createDatabase( "foo", cluster );

        // then: it should appear on the alive members
        assertDatabaseEventuallyStarted( "foo", aliveMembers );

        // when: starting the dead members
        deadMembers.forEach( ClusterMember::start );

        // then: the database should appear started everywhere
        assertDatabaseEventuallyStarted( "foo", cluster );
    }

    @ParameterizedTest( name = "initialCores={0} coresToShutdown={1} initialReplicas={2} replicasToShutdown={3}" )
    @MethodSource( "clusterConfig" )
    void shouldStartDatabaseWithReducedAvailability( int initialCores, int coresToShutdown, int initialReplicas, int replicasToShutdown ) throws Exception
    {
        // given: a cluster with a created but stopped database and a few dead members
        Cluster cluster = startCluster( initialCores, initialReplicas );

        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );

        stopDatabase( "foo", cluster );
        assertDatabaseEventuallyStopped( "foo", cluster );

        Set<ClusterMember> deadMembers = shutdownMembers( cluster, coresToShutdown, replicasToShutdown );
        Set<ClusterMember> aliveMembers = aliveMembers( cluster, deadMembers );

        // when: starting the database
        startDatabase( "foo", cluster );

        // then: it should be started on the alive members
        assertDatabaseEventuallyStarted( "foo", aliveMembers );

        // when: starting the dead members
        deadMembers.forEach( ClusterMember::start );

        // then: the database should appear started everywhere
        assertDatabaseEventuallyStarted( "foo", cluster );
    }

    @ParameterizedTest( name = "initialCores={0} coresToShutdown={1} initialReplicas={2} replicasToShutdown={3}" )
    @MethodSource( "clusterConfig" )
    void shouldStopDatabaseWithReducedAvailability( int initialCores, int coresToShutdown, int initialReplicas, int replicasToShutdown ) throws Exception
    {
        // given: a cluster with a started database and a few dead members
        Cluster cluster = startCluster( initialCores, initialReplicas );

        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );

        Set<ClusterMember> deadMembers = shutdownMembers( cluster, coresToShutdown, replicasToShutdown );
        Set<ClusterMember> aliveMembers = aliveMembers( cluster, deadMembers );

        // when: stopping the database
        stopDatabase( "foo", cluster );

        // then: it should be stopped on the alive members
        assertDatabaseEventuallyStopped( "foo", aliveMembers );

        // when: starting the dead members
        deadMembers.forEach( ClusterMember::start );

        // then: the database should appear stopped everywhere
        assertDatabaseEventuallyStopped( "foo", cluster );
    }

    @ParameterizedTest( name = "initialCores={0} coresToShutdown={1} initialReplicas={2} replicasToShutdown={3}" )
    @MethodSource( "clusterConfig" )
    void shouldDropDatabaseWithReducedAvailability( int initialCores, int coresToShutdown, int initialReplicas, int replicasToShutdown ) throws Exception
    {
        // given: a cluster with a started database and a few dead members
        Cluster cluster = startCluster( initialCores, initialReplicas );

        createDatabase( "foo", cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );

        Set<ClusterMember> deadMembers = shutdownMembers( cluster, coresToShutdown, replicasToShutdown );
        Set<ClusterMember> aliveMembers = aliveMembers( cluster, deadMembers );

        // when: dropping the database
        dropDatabase( "foo", cluster, false );

        // then: it should be dropped on the alive members
        assertDatabaseEventuallyDoesNotExist( "foo", aliveMembers );

        // when: starting the dead members
        deadMembers.forEach( ClusterMember::start );

        // then: the database should appear dropped everywhere
        assertDatabaseEventuallyDoesNotExist( "foo", cluster );
    }

    private Set<ClusterMember> aliveMembers( Cluster cluster, Set<ClusterMember> deadMembers )
    {
        return cluster.allMembers().stream().filter( m -> !deadMembers.contains( m ) ).collect( toSet() );
    }

    private static Set<ClusterMember> shutdownMembers( Cluster cluster, int coresToShutdown, int replicasToShutdown )
    {
        Set<ClusterMember> deadMembers = concat(
                cluster.coreMembers().stream().limit( coresToShutdown ),
                cluster.readReplicas().stream().limit( replicasToShutdown ) )
                .collect( toSet() );
        deadMembers.forEach( ClusterMember::shutdown );
        return deadMembers;
    }
}
