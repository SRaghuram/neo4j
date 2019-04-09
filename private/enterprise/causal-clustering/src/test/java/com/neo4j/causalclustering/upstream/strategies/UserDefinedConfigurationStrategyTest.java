/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.collection.Iterators.asSet;

class UserDefinedConfigurationStrategyTest
{
    private static final DatabaseId DATABASE_ID = new DatabaseId( "customers" );

    private final String northGroup = "north";
    private final String southGroup = "south";
    private final String westGroup = "west";
    private final String eastGroup = "east";
    private final List<String> noEastGroup = List.of( northGroup, southGroup, westGroup );

    @Test
    void shouldPickTheFirstMatchingServerIfCore()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService =
                fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ), this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, "groups(east); groups(core); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, contains( theCoreMemberId ) );
    }

    @Test
    void shouldPickTheFirstMatchingServerIfReadReplica()
    {
        // given
        MemberId[] readReplicaIds = memberIDs( 100 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        String wantedGroup = noEastGroup.get( 1 );
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, contains( isIn( readReplicaIds ) ) );
        assertThat( memberId.map( this::noEastGroupGenerator ), contains( equalTo( asSet( wantedGroup ) ) ) );
    }

    @Test
    void shouldReturnEmptyIfNoMatchingServers()
    {
        // given
        MemberId[] readReplicaIds = memberIDs( 100 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        String wantedGroup = eastGroup;
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, empty() );
    }

    @Test
    void shouldReturnEmptyIfInvalidFilterSpecification()
    {
        // given
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( memberIDs( 100 ), this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "invalid filter specification" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, empty() );
    }

    @Test
    void shouldNotReturnSelf()
    {
        // given
        String wantedGroup = eastGroup;
        MemberId[] readReplicaIds = memberIDs( 1 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, memberId -> asSet( wantedGroup ) ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), readReplicaIds[0] );

        //when
        Optional<MemberId> memberId = strategy.upstreamMemberForDatabase( DATABASE_ID );

        // then
        assertThat( memberId, empty() );
    }

    private Config configWithFilter( String filter )
    {
        return Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, filter );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( MemberId... readReplicaIds )
    {
        return fakeReadReplicaTopology( readReplicaIds, ignored -> Collections.emptySet() );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( MemberId[] readReplicaIds, Function<MemberId,Set<String>> groupGenerator )
    {
        assert readReplicaIds.length > 0;

        final AtomicInteger offset = new AtomicInteger( 10_000 );

        Function<MemberId,ReadReplicaInfo> toReadReplicaInfo = memberId -> readReplicaInfo( memberId, offset, groupGenerator );

        Map<MemberId,ReadReplicaInfo> readReplicas = Stream.of( readReplicaIds ).collect( Collectors.toMap( Function.identity(), toReadReplicaInfo ) );

        return new ReadReplicaTopology( readReplicas );
    }

    private static ReadReplicaInfo readReplicaInfo( MemberId memberId, AtomicInteger offset, Function<MemberId,Set<String>> groupGenerator )
    {
        ClientConnectorAddresses connectorAddresses = new ClientConnectorAddresses( List.of(
                new ConnectorUri( bolt, new AdvertisedSocketAddress( "localhost", offset.getAndIncrement() ) ) ) );
        AdvertisedSocketAddress catchupAddress = new AdvertisedSocketAddress( "localhost", offset.getAndIncrement() );
        Set<String> groups = groupGenerator.apply( memberId );
        Set<DatabaseId> databaseIds = Set.of( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        return new ReadReplicaInfo( connectorAddresses, catchupAddress, groups, databaseIds );
    }

    private static Map<MemberId,AdvertisedSocketAddress> extractCatchupAddressesMap( CoreTopology coreTopology, ReadReplicaTopology rrTopology )
    {
        Map<MemberId,AdvertisedSocketAddress> catchupAddressMap = new HashMap<>();

        for ( Map.Entry<MemberId,CoreServerInfo> entry : coreTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().getCatchupServer() );
        }

        for ( Map.Entry<MemberId,ReadReplicaInfo> entry : rrTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().getCatchupServer() );

        }

        return catchupAddressMap;
    }

    static TopologyService fakeTopologyService( CoreTopology coreTopology, ReadReplicaTopology readReplicaTopology )
    {
        return new TopologyService()
        {
            private final DatabaseId DATABASE_ID = new DatabaseId( "default" );

            private Map<MemberId,AdvertisedSocketAddress> catchupAddresses = extractCatchupAddressesMap( coreTopology, readReplicaTopology );

            @Override
            public CoreTopology allCoreServers()
            {
                return coreTopology;
            }

            @Override
            public ReadReplicaTopology allReadReplicas()
            {
                return readReplicaTopology;
            }

            @Override
            public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
            {
                AdvertisedSocketAddress advertisedSocketAddress = catchupAddresses.get( upstream );
                if ( advertisedSocketAddress == null )
                {
                    throw new CatchupAddressResolutionException( upstream );
                }
                return advertisedSocketAddress;
            }

            @Override
            public Map<MemberId,RoleInfo> allCoreRoles()
            {
                return emptyMap();
            }

            @Override
            public MemberId myself()
            {
                return new MemberId( new UUID( 0, 0 ) );
            }

            @Override
            public DatabaseId localDatabaseId()
            {
                return DATABASE_ID;
            }

            @Override
            public void init()
            {
            }

            @Override
            public void start()
            {
            }

            @Override
            public void stop()
            {
            }

            @Override
            public void shutdown()
            {
            }
        };
    }

    static MemberId[] memberIDs( int howMany )
    {
        return Stream.generate( () -> new MemberId( UUID.randomUUID() ) ).limit( howMany ).toArray( MemberId[]::new );
    }

    private Set<String> noEastGroupGenerator( MemberId memberId )
    {
        int index = Math.abs( memberId.hashCode() % noEastGroup.size() );
        return asSet( noEastGroup.get( index ) );
    }
}
