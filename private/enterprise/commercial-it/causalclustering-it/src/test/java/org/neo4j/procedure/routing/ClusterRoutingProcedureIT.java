/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.Reliable.SHARED;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.helpers.SocketAddressParser.socketAddress;

@ClusterExtension
class ClusterRoutingProcedureIT extends BaseRoutingProcedureIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    static void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig() );
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
    @Disabled( "For now, cores do not filter the addresses received from discovery service by database name" )
    void shouldCallRoutingProcedureWithInvalidDatabaseNameOnCores()
    {
        String unknownDatabaseName = "non_existing_core_database";

        assertAll( cluster.coreMembers().stream().map( core ->
                () -> assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, core.defaultDatabase() ) ) );
    }

    @Test
    void shouldCallRoutingProcedureWithInvalidDatabaseNameOnReadReplicas()
    {
        String unknownDatabaseName = "non_existing_replica_database";

        assertAll( cluster.readReplicas().stream().map( readReplica ->
                () -> assertRoutingProceduresFailForUnknownDatabase( unknownDatabaseName, readReplica.defaultDatabase() ) ) );
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

    private Executable assertRoutingProceduresAvailable( CoreClusterMember coreMember )
    {
        return assertRoutingProceduresAvailable( coreMember, null );
    }

    private Executable assertRoutingProceduresAvailable( CoreClusterMember coreMember, String databaseName )
    {
        return () ->
        {
            CoreClusterMember leader = cluster.awaitLeader();

            List<AdvertisedSocketAddress> writers = singletonList( boltAddress( leader ) );

            List<AdvertisedSocketAddress> readers = Stream.concat( cluster.coreMembers().stream(), cluster.readReplicas().stream() )
                    .filter( member -> !member.equals( leader ) ) // leader is a writer and router, not a reader
                    .map( this::boltAddress )
                    .collect( toList() );

            List<AdvertisedSocketAddress> routers = cluster.coreMembers()
                    .stream()
                    .map( this::boltAddress )
                    .collect( toList() );

            Duration ttl = Config.defaults().get( routing_ttl );
            RoutingResult expectedResult = new RoutingResult( routers, writers, readers, ttl.getSeconds() );

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
            AdvertisedSocketAddress address = boltAddress( readReplica );

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

    private AdvertisedSocketAddress boltAddress( ClusterMember member )
    {
        return socketAddress( member.boltAdvertisedAddress(), AdvertisedSocketAddress::new );
    }

    private static ClusterConfig clusterConfig()
    {
        return ClusterConfig.clusterConfig()
                .withNumberOfReadReplicas( 2 )
                .withDiscoveryServiceType( SHARED );
    }
}
