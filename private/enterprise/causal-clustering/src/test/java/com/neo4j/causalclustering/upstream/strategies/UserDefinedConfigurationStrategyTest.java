/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import co.unruly.matchers.OptionalMatchers;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class UserDefinedConfigurationStrategyTest
{
    private static final NamedDatabaseId DATABASE_ID = randomNamedDatabaseId();

    private final String northGroup = "north";
    private final String southGroup = "south";
    private final String westGroup = "west";
    private final String eastGroup = "east";
    private final List<ServerGroupName> noEastGroup = ServerGroupName.listOf( northGroup, southGroup, westGroup );

    @Test
    void shouldPickTheFirstMatchingServerIfCore()
    {
        // given
        ServerId theCoreServerId = serverId( 0 );
        Set<ServerId> replicaIds = serverIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreServerId ), replicaIds, theCoreServerId, Set.of( DATABASE_ID ) );

        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, "groups(east); groups(core); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<ServerId> serverId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> serverIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( serverIds, contains( theCoreServerId ) );
        assertThat( serverId, OptionalMatchers.contains( theCoreServerId ) );
    }

    @Test
    void shouldPickTheFirstMatchingServerIfReadReplica()
    {
        // given
        ServerId theCoreServerId = serverId( 0 );
        Set<ServerId> replicaIds = serverIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreServerId ), replicaIds, theCoreServerId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        var wantedGroup = noEastGroup.get( 1 );
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, everyItem( is( in( replicaIds ) ) ) );
        assertThat( memberId, OptionalMatchers.contains( is( in( replicaIds ) ) ) );
        assertThat( memberId.map( this::noEastGroupGenerator ), OptionalMatchers.contains( equalTo( wantedGroup ) ) );
        var memberGroups = memberIds.stream().map( this::noEastGroupGenerator ).collect( Collectors.toList() );
        assertThat( memberGroups, everyItem( equalTo( wantedGroup ) ) );
    }

    @Test
    void shouldReturnEmptyIfNoMatchingServers()
    {
        // given
        ServerId theCoreServerId = serverId( 0 );
        Set<ServerId> replicaIds = serverIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreServerId ), replicaIds, theCoreServerId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + eastGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    @Test
    void shouldReturnEmptyIfInvalidFilterSpecification()
    {
        // given
        ServerId theCoreServerId = serverId( 0 );
        Set<ServerId> replicaIds = serverIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreServerId ), replicaIds, theCoreServerId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "invalid filter specification" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    @Test
    void shouldNotReturnSelf()
    {
        // given
        var wantedGroup = new ServerGroupName( eastGroup );
        ServerId coreId = serverId( 0 );
        ServerId replicaId = serverId( 1 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( coreId ), Set.of( replicaId ), replicaId, Set.of( DATABASE_ID ) );

        topologyService.setGroups( Set.of( replicaId ), Set.of( wantedGroup ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), replicaId );

        //when
        Optional<ServerId> memberId = strategy.upstreamServerForDatabase( DATABASE_ID );
        Collection<ServerId> memberIds = strategy.upstreamServersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    private Config configWithFilter( String filter )
    {
        return Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, filter );
    }

    private void randomlyAssignGroupsNoEast( FakeTopologyService topologyService, Set<ServerId> memberIds )
    {
        var groupToMembers = memberIds.stream().map( memberId -> Pair.of( noEastGroupGenerator( memberId ), List.of( memberId ) ) ).collect(
                Collectors.toMap( Pair::first, Pair::other, this::memberIdsAccumulator ) );

        groupToMembers.forEach( ( group, members ) -> topologyService.setGroups( Set.copyOf( members ), Set.of( group ) ) );
    }

    private List<ServerId> memberIdsAccumulator( List<ServerId> l, List<ServerId> r )
    {
        return Stream.concat( l.stream(), r.stream() ).collect( Collectors.toList() );
    }

    private ServerGroupName noEastGroupGenerator( ServerId serverId )
    {
        // Random selection of non-east group, stable w.r.t ServerId
        int index = Math.abs( serverId.hashCode() % noEastGroup.size() );
        return noEastGroup.get( index );
    }
}
