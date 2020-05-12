/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import co.unruly.matchers.OptionalMatchers;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.ConnectorAddresses.ConnectorUri;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberIds;
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
        MemberId theCoreMemberId = memberId( 0 );
        Set<MemberId> replicaIds = memberIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreMemberId ), replicaIds, theCoreMemberId, Set.of( DATABASE_ID ) );

        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, "groups(east); groups(core); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberIds, contains( theCoreMemberId ) );
        assertThat( memberId, OptionalMatchers.contains( theCoreMemberId ) );
    }

    @Test
    void shouldPickTheFirstMatchingServerIfReadReplica()
    {
        // given
        MemberId theCoreMemberId = memberId( 0 );
        Set<MemberId> replicaIds = memberIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreMemberId ), replicaIds, theCoreMemberId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        var wantedGroup = noEastGroup.get( 1 );
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

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
        MemberId theCoreMemberId = memberId( 0 );
        Set<MemberId> replicaIds = memberIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreMemberId ), replicaIds, theCoreMemberId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + eastGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    @Test
    void shouldReturnEmptyIfInvalidFilterSpecification()
    {
        // given
        MemberId theCoreMemberId = memberId( 0 );
        Set<MemberId> replicaIds = memberIds( 1, 101 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( theCoreMemberId ), replicaIds, theCoreMemberId, Set.of( DATABASE_ID ) );
        randomlyAssignGroupsNoEast( topologyService, replicaIds );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "invalid filter specification" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    @Test
    void shouldNotReturnSelf()
    {
        // given
        var wantedGroup = new ServerGroupName( eastGroup );
        MemberId coreId = memberId( 0 );
        MemberId replicaId = memberId( 1 );
        FakeTopologyService topologyService = new FakeTopologyService( Set.of( coreId ), Set.of( replicaId ), replicaId, Set.of( DATABASE_ID ) );

        topologyService.setGroups( Set.of( replicaId ), Set.of( wantedGroup ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), replicaId );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );
        Collection<MemberId> memberIds = strategy.upstreamMembersForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, OptionalMatchers.empty() );
        assertThat( memberIds, empty() );
    }

    private Config configWithFilter( String filter )
    {
        return Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, filter );
    }

    static DatabaseReadReplicaTopology fakeReadReplicaTopology( MemberId... readReplicaIds )
    {
        return fakeReadReplicaTopology( readReplicaIds, ignored -> Collections.emptySet() );
    }

    static DatabaseReadReplicaTopology fakeReadReplicaTopology( MemberId[] readReplicaIds, Function<MemberId,Set<ServerGroupName>> groupGenerator )
    {
        assert readReplicaIds.length > 0;

        final AtomicInteger offset = new AtomicInteger( 10_000 );

        Function<MemberId,ReadReplicaInfo> toReadReplicaInfo = memberId -> readReplicaInfo( memberId, offset, groupGenerator );

        Map<MemberId,ReadReplicaInfo> readReplicas = Stream.of( readReplicaIds ).collect( Collectors.toMap( Function.identity(), toReadReplicaInfo ) );

        return new DatabaseReadReplicaTopology( randomNamedDatabaseId().databaseId(), readReplicas );
    }

    private static ReadReplicaInfo readReplicaInfo( MemberId memberId, AtomicInteger offset, Function<MemberId,Set<ServerGroupName>> groupGenerator )
    {
        ConnectorAddresses connectorAddresses = ConnectorAddresses.fromList( List.of(
                new ConnectorUri( bolt, new SocketAddress( "localhost", offset.getAndIncrement() ) ) ) );
        SocketAddress catchupAddress = new SocketAddress( "localhost", offset.getAndIncrement() );
        var groups = groupGenerator.apply( memberId );
        Set<DatabaseId> databaseIds = Set.of( randomNamedDatabaseId().databaseId() );
        return new ReadReplicaInfo( connectorAddresses, catchupAddress, groups, databaseIds );
    }

    private void randomlyAssignGroupsNoEast( FakeTopologyService topologyService, Set<MemberId> memberIds )
    {
        var groupToMembers = memberIds.stream()
                .map( memberId -> Pair.of( noEastGroupGenerator( memberId ), List.of( memberId ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other, this::memberIdsAccumulator ) );

        groupToMembers.forEach( ( group, members ) -> topologyService.setGroups( Set.copyOf( members ), Set.of( group ) ) );
    }

    private List<MemberId> memberIdsAccumulator( List<MemberId> l, List<MemberId> r )
    {
        return Stream.concat( l.stream(), r.stream() ).collect( Collectors.toList() );
    }

    private ServerGroupName noEastGroupGenerator( MemberId memberId )
    {
        // Random selection of non-east group, stable w.r.t MemberId
        int index = Math.abs( memberId.hashCode() % noEastGroup.size() );
        return noEastGroup.get( index );
    }
}
