/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_leader;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.helpers.SocketAddressParser.socketAddress;

@ClusterExtension
class ClusterRoutingProcedureIT extends BaseRoutingProcedureIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfReadReplicas( 2 ) );
        cluster.start();
    }

    @Test
    void shouldExposeRoutingProceduresOnCores()
    {
        assertAll( cluster.coreMembers().stream().map( this::assertRoutingProceduresAvailable ) );
    }

    @Test
    void shouldExposeRoutingProceduresOnReadReplicas()
    {
        assertAll( cluster.readReplicas().stream().map( this::assertRoutingProceduresAvailable ) );
    }

    @Test
    void shouldCallRoutingProcedureWithValidDatabaseNameOnCores()
    {
        String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

        assertAll( cluster.coreMembers().stream().map( core -> assertRoutingProceduresAvailable( core, databaseName ) ) );
    }

    @Test
    void shouldCallRoutingProcedureWithValidDatabaseNameOnReadReplicas()
    {
        String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

        assertAll( cluster.readReplicas().stream().map( readReplica -> assertRoutingProceduresAvailable( readReplica, databaseName ) ) );
    }

    @Test
    void shouldCallRoutingProcedureWithNonExistingDatabaseNameOnCores()
    {
        String unknownDatabaseName = "non-existing-core-database";

        assertAll( cluster.coreMembers().stream().map( core ->
                () -> assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, core.defaultDatabase() ) ) );
    }

    @Test
    void shouldCallRoutingProcedureWithNonExistingDatabaseNameOnReadReplicas()
    {
        String unknownDatabaseName = "non-existing-replica-database";

        assertAll( cluster.readReplicas().stream().map( readReplica ->
                () -> assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, readReplica.defaultDatabase() ) ) );
    }

    @Test
    void shouldSuccessfullyCallRoutingProcedureDespiteStoppedDatabase() throws Exception
    {
        String databaseName = "stopped-database";

        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        stopDatabase( databaseName, cluster );
        assertDatabaseEventuallyStopped( databaseName, cluster );

        assertAll( cluster.coreMembers().stream().map( this::assertRoutingProceduresAvailable ) );
    }

    @Test
    void shouldNotAllowDriverToUseNonExistingDatabaseOnCores()
    {
        String unknownDatabaseName = "non-existing-core-database";

        assertAll( cluster.coreMembers().stream().map( core ->
                () -> assertRoutingDriverFailsForUnknownDatabase( core.boltAdvertisedAddress(), unknownDatabaseName ) ) );
    }

    @Test
    void shouldNotAllowDriverToUseNonExistingDatabaseOnReadReplicas()
    {
        String unknownDatabaseName = "non-existing-replica-database";

        assertAll( cluster.readReplicas().stream().map( readReplica ->
                () -> assertRoutingDriverFailsForUnknownDatabase( readReplica.boltAdvertisedAddress(), unknownDatabaseName ) ) );
    }

    @Test
    void shouldAllowRoutingDriverToReadAndWriteWhenCreatedWithCoreAddress()
    {
        assertAll( cluster.coreMembers()
                .stream()
                .map( member -> () -> assertPossibleToReadAndWriteUsingRoutingDriver( member.boltAdvertisedAddress() ) ) );
    }

    @Test
    void shouldAllowRoutingDriverToReadWhenCreatedWithReadReplicaAddress()
    {
        assertAll( cluster.readReplicas()
                .stream()
                .map( member -> () -> assertPossibleToReadUsingRoutingDriver( member.boltAdvertisedAddress() ) ) );
    }

    @Test
    void shouldNotAllowRoutingDriverToWriteWhenCreatedWithReadReplicaAddress()
    {
        assertAll( cluster.readReplicas()
                .stream()
                .map( member -> () -> assertNotPossibleToWriteUsingRoutingDriver( member.boltAdvertisedAddress() ) ) );
    }

    @Test
    void allowingLeaderAsReadEndpointDynamicallyShouldReturnAllMembersAsReadEndpoints()
    {
        setAllowReadFromLeader( true );
        assertAll( cluster.coreMembers().stream().map( this::assertRoutingProceduresAvailableWithAllowReadFromLeader ) );
        setAllowReadFromLeader( false );
        assertAll( cluster.coreMembers().stream().map( this::assertRoutingProceduresAvailable ) );
    }

    private Executable assertRoutingProceduresAvailable( CoreClusterMember coreMember )
    {
        return assertRoutingProceduresAvailable( coreMember, null );
    }

    private Executable assertRoutingProceduresAvailableWithAllowReadFromLeader( CoreClusterMember coreMember )
    {
        return assertRoutingProceduresAvailable( coreMember, null, true );
    }

    private Executable assertRoutingProceduresAvailable( CoreClusterMember coreMember, String databaseName )
    {
        return assertRoutingProceduresAvailable( coreMember, databaseName, false );
    }

    private Executable assertRoutingProceduresAvailable( CoreClusterMember coreMember, String databaseName, boolean allowReadFromLeader )
    {
        return () ->
        {
            var leader = cluster.awaitLeader();

            var writers = singletonList( boltAddress( leader ) );

            var readers = Stream.concat( cluster.coreMembers().stream(), cluster.readReplicas().stream() )
                    .filter( member -> allowReadFromLeader || !member.equals( leader ) ) // leader is a writer and router, not a reader (unless enabled)
                    .map( this::boltAddress )
                    .collect( toList() );

            var routers = cluster.coreMembers()
                    .stream()
                    .map( this::boltAddress )
                    .collect( toList() );

            var ttl = Config.defaults().get( routing_ttl );
            var expectedResult = new RoutingResult( routers, writers, readers, ttl.getSeconds() );

            if ( databaseName != null )
            {
                assertRoutingProceduresAvailable( databaseName, coreMember.defaultDatabase(), expectedResult );
            }
            else
            {
                assertRoutingProceduresAvailable( coreMember.defaultDatabase(), expectedResult );
            }
        };
    }

    private Executable assertRoutingProceduresAvailable( ReadReplica readReplica )
    {
        return assertRoutingProceduresAvailable( readReplica, null );
    }

    private Executable assertRoutingProceduresAvailable( ReadReplica readReplica, String databaseName )
    {
        return () ->
        {
            SocketAddress address = boltAddress( readReplica );

            Duration ttl = Config.defaults().get( routing_ttl );
            RoutingResult expectedResult = new RoutingResult( singletonList( address ), emptyList(), singletonList( address ), ttl.getSeconds() );

            if ( databaseName != null )
            {
                assertRoutingProceduresAvailable( databaseName, readReplica.defaultDatabase(), expectedResult );
            }
            else
            {
                assertRoutingProceduresAvailable( readReplica.defaultDatabase(), expectedResult );
            }
        };
    }

    private SocketAddress boltAddress( ClusterMember member )
    {
        return socketAddress( member.boltAdvertisedAddress(), SocketAddress::new );
    }

    private void setAllowReadFromLeader( boolean allowReadFromLeader )
    {
        cluster.allMembers().forEach( member -> {
            member.defaultDatabase().executeTransactionally( "CALL dbms.setConfigValue($key,$value)",
                    Map.of("key", cluster_allow_reads_on_leader.name(), "value", Boolean.toString( allowReadFromLeader ) ) );
        } );
    }
}
