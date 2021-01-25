/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.TopologyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.akka.BatchingMultiDatabaseLogger.newPaddedLine;
import static com.neo4j.causalclustering.discovery.akka.BatchingMultiDatabaseLoggerTest.sortedSetCollector;
import static java.lang.System.lineSeparator;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.logging.LogAssertions.assertThat;

@SuppressWarnings( "unused" )
class TopologyLoggerTest
{
    private static final String CORE_TOPOLOGY_DESCRIPTION = "core topology";
    private static final String RR_TOPOLOGY_DESCRIPTION = "read replica topology";
    private static final int MEMBERS_PER_ROLE = 3;
    private static final TopologyChangePair<DatabaseCoreTopology> coreChangeFactory = TopologyLoggerTest::newOldCoreTopology;
    private static final TopologyChangePair<DatabaseReadReplicaTopology> rrChangeFactory = TopologyLoggerTest::newOldReplicaTopology;

    static Stream<Arguments> topologyTypes()
    {
        return Stream.of(
                Arguments.of( coreChangeFactory, 0, CORE_TOPOLOGY_DESCRIPTION, "coreTest" ),
                Arguments.of( rrChangeFactory, MEMBERS_PER_ROLE, RR_TOPOLOGY_DESCRIPTION, "readReplicaTest" )
        );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void equalTopologiesShouldReturnEmpty( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // setup
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var topologyService = new FakeTopologyService( serverIds( 0, MEMBERS_PER_ROLE ),
                                                       serverIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       serverId( 0 ),
                                                       namedDatabaseIds );
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var topologyLogger = new TopologyLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var newTopology = topologyChangePair.create( topologyService, serverId( 0 ) , namedDatabaseIds.first() ).first();
        var oldTopology = topologyChangePair.create( topologyService, serverId( 0 ) , namedDatabaseIds.first() ).first();

        // when new and old topologies contains same info
        var result = topologyLogger.computeChange( topologyDescription, newTopology, oldTopology );

        // the should return empty optional
        assertThat( result.isEmpty() ).isTrue();
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldReturnNewMemberSpecification( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // setup
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var topologyService = new FakeTopologyService( serverIds( 0, MEMBERS_PER_ROLE ),
                                                       serverIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       serverId( 0 ),
                                                       namedDatabaseIds );
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var topologyLogger = new TopologyLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var newOldTopology = topologyChangePair.create( topologyService, serverId( roleOffset ) , namedDatabaseIds.first() );

        // when topology has a new member
        var sortedRemainingMembers = new TreeSet<>( Comparator.comparing( ServerId::uuid ) );
        sortedRemainingMembers.addAll( serverIds( 0, 3, roleOffset ) );
        var result = topologyLogger.computeChange( topologyDescription, newOldTopology.first(), newOldTopology.other() );

        // then should print the new member
        assertThat( result.isPresent() ).isTrue();
        var expectedSpecification = "is now: " +
                                    sortedRemainingMembers +
                                    lineSeparator() +
                                    "No servers where lost" +
                                    lineSeparator() +
                                    "New servers: " +
                                    newPaddedLine() +
                                    serverId( roleOffset ) + "=" + newOldTopology.first().servers().get( serverId( roleOffset ) );
        assertThat( result.get().specification() ).isEqualTo( expectedSpecification );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldReturnLostMemberSpecification( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // setup
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var topologyService = new FakeTopologyService( serverIds( 0, MEMBERS_PER_ROLE ),
                                                       serverIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       serverId( 0 ),
                                                       namedDatabaseIds );
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var topologyLogger = new TopologyLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var newOldTopology = topologyChangePair.create( topologyService, serverId( roleOffset ) , namedDatabaseIds.first() );

        // when topology lost a new member
        var sortedRemainingMembers = new TreeSet<>( Comparator.comparing( ServerId::uuid ) );
        sortedRemainingMembers.addAll( serverIds( 1, 3, roleOffset ) );
        var result = topologyLogger.computeChange( topologyDescription, newOldTopology.other(), newOldTopology.first() );

        // then should print the lost member
        assertThat( result.isPresent() ).isTrue();
        var expectedSpecification = "is now: " +
                                    sortedRemainingMembers +
                                    lineSeparator() +
                                    "Lost servers: " +
                                    Set.of( serverId( roleOffset ) ) +
                                    lineSeparator() +
                                    "No new servers";
        assertThat( result.get().specification() ).isEqualTo( expectedSpecification );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldReturnTitle( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // setup
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var topologyService = new FakeTopologyService( serverIds( 0, MEMBERS_PER_ROLE ),
                                                       serverIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       serverId( 0 ),
                                                       namedDatabaseIds );
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var topologyLogger = new TopologyLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var newOldTopology = topologyChangePair.create( topologyService, serverId( roleOffset ) , namedDatabaseIds.first() );

        // when topology has changed
        var result = topologyLogger.computeChange( topologyDescription, newOldTopology.other(), newOldTopology.first() );

        // then should return ChangeKey in an Optional, with correct title
        assertThat( result.isPresent() ).isTrue();
        assertThat( result.get().title() ).isEqualTo( topologyDescription );
    }

    @Test
    void shouldReturnDatabaseId()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var topologyLogger = new TopologyLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var coreDbIdIn = randomDatabaseId();
        var rrDbIdIn = randomDatabaseId();

        var coreTopology = DatabaseCoreTopology.empty( coreDbIdIn );
        var rrTopology = DatabaseReadReplicaTopology.empty( rrDbIdIn );

        // when
        var coreDbIdOut = topologyLogger.extractDatabaseId( coreTopology );
        var rrDbIdOut = topologyLogger.extractDatabaseId( rrTopology );

        // then
        assertThat( coreDbIdOut ).isEqualTo( coreDbIdIn );
        assertThat( rrDbIdOut ).isEqualTo( rrDbIdIn );
    }

    @FunctionalInterface
    interface TopologyChangePair<T extends Topology<? extends DiscoveryServerInfo>>
    {
        Pair<T,T> create( TopologyService topologyService, ServerId removed, NamedDatabaseId database );
    }

    private static Pair<DatabaseCoreTopology,DatabaseCoreTopology> newOldCoreTopology( TopologyService topologyService, ServerId removed,
            NamedDatabaseId databaseId )
    {
        var largerTopology = topologyService.coreTopologyForDatabase( databaseId );

        var membersMinusRemoved = new HashMap<>( largerTopology.servers() );
        membersMinusRemoved.remove( removed );

        var smallerTopology = new DatabaseCoreTopology( databaseId.databaseId(), largerTopology.raftGroupId(), membersMinusRemoved );
        return Pair.of( largerTopology, smallerTopology );
    }

    private static Pair<DatabaseReadReplicaTopology,DatabaseReadReplicaTopology> newOldReplicaTopology( TopologyService topologyService, ServerId removed,
            NamedDatabaseId databaseId )
    {
        var largerTopology = topologyService.readReplicaTopologyForDatabase( databaseId );

        var membersMinusRemoved = new HashMap<>( largerTopology.servers() );
        membersMinusRemoved.remove( removed );

        var smallerTopology = new DatabaseReadReplicaTopology( databaseId.databaseId(), membersMinusRemoved );
        return Pair.of( largerTopology, smallerTopology );
    }

    /* Note generators below produce sorted collections because we need stable orderings for asserting on log message contents */

    private static SortedSet<NamedDatabaseId> generateNamedDatabaseIds( int size )
    {
        return Stream.generate( TestDatabaseIdRepository::randomNamedDatabaseId )
                     .limit( size )
                     .collect( sortedSetCollector( namedDbId -> namedDbId.databaseId().uuid() ) );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static SortedSet<ServerId> serverIds( int from, int until, int roleOffset )
    {
        return serverIds( from + roleOffset, until + roleOffset );
    }

    private static SortedSet<ServerId> serverIds( int from, int until )
    {
        var result = new TreeSet<>( Comparator.comparing( ServerId::uuid ) );
        result.addAll( FakeTopologyService.serverIds( from, until ) );
        return result;
    }
}
