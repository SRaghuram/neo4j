/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimeoutHandler;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static org.mockito.Mockito.spy;
import static org.neo4j.logging.LogAssertions.assertThat;

@SuppressWarnings( "unused" )
class TopologyLoggerTest
{
    private static final String CORE_TOPOLOGY_DESCRIPTION = "Core topology";
    private static final String RR_TOPOLOGY_DESCRIPTION = "Read replica topology";
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
    void shouldLogAndFlushAfterTimeout( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given some topology changes
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE,2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        namedDatabaseIds.stream()
                        .map( db -> topologyChangePair.create( topologyService, memberId( roleOffset ), db ) )
                        .forEach( pair -> topologyLogger.logTopologyChange( topologyDescription, pair.first(), pair.other() ) );

        // when the timer is fired
        assertThat( logger ).doesNotHaveAnyLogs();
        timerService.getTimer().invoke();

        // then those topology changes should be logged
        var sortedRemainingMembers = new TreeSet<>( Comparator.comparing( MemberId::getUuid ) );
        sortedRemainingMembers.addAll( memberIds( 1, 3, roleOffset ) );
        var expectedMessage = String.format( "%s for all databases is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members", topologyDescription, sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );

        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );

        // when the timer is fired again without subsequent topology changes
        logger.clear();
        timerService.getTimer().invoke();

        // then nothing should be logged
        assertThat( logger ).doesNotHaveAnyLogs();
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void newBatchKeyShouldTriggerFlush( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 5 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE,2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when batching with one batchKey
        var loggedDatabaseIds = new ArrayList<DatabaseId>( 2 );
        namedDatabaseIds.stream()
                        .limit( 2 )
                        .peek( namedDatabaseId -> loggedDatabaseIds.add( namedDatabaseId.databaseId() ) )
                        .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseId ) )
                        .forEach( topology -> topologyLogger.logTopologyChange( topologyDescription, topology.first(), topology.other() ) );

        // then nothing should be logged
        assertThat( logger ).doesNotHaveAnyLogs();

        // when a new batchKey is submitted
        namedDatabaseIds.stream()
                        .skip( 2 )
                        .limit( 2 )
                        .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset + 1 ), namedDatabaseId ) )
                        .forEach( topology -> topologyLogger.logTopologyChange( topologyDescription, topology.first(), topology.other() ) );

        // then first batchKey should be flushed
        var sortedRemainingMembers = memberIds( roleOffset + 1, roleOffset + 3 );
        var expectedMessage = String.format( "%s for databases %s and %s is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             topologyDescription, loggedDatabaseIds.get( 0 ), loggedDatabaseIds.get( 1 ),
                                             sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldLogOneExplicitDBName( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when timer is fired after topology change for one database
        var newOldTopology = topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseIds.first() );
        topologyLogger.logTopologyChange( topologyDescription, newOldTopology.first(), newOldTopology.other() );
        timerService.getTimer().invoke();

        // then log should contain one explicit database name
        var sortedRemainingMembers = memberIds( 1, 3, roleOffset );
        var expectedMessage = String.format( "%s for database %s is now: %s" +
                                         System.lineSeparator() +
                                         "Lost members :%s" +
                                         System.lineSeparator() +
                                         "No new members",
                                         topologyDescription, namedDatabaseIds.first().databaseId(),
                                             sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldLogAllDatabasesBatchedTogether( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE,2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when timer is fired after topology change for all database
        namedDatabaseIds.stream()
                        .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseId ) )
                        .forEach( pair -> topologyLogger.logTopologyChange( topologyDescription, pair.first(), pair.other() ) );
        timerService.getTimer().invoke();

        // then log should contain changes for "all databases"
        var sortedRemainingMembers = memberIds( 1, 3, roleOffset );
        var expectedMessage = String.format( "%s for all databases is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             topologyDescription, sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldLogWithDatabasesNotChanged( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 5 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when timer is fired after topology change for 4 out of 5 databases
        namedDatabaseIds.stream()
                        .filter( namedDatabaseId -> !namedDatabaseId.equals( namedDatabaseIds.first() ) )
                        .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseId ) )
                        .forEach( pair -> topologyLogger.logTopologyChange( topologyDescription, pair.first(), pair.other() ) );
        timerService.getTimer().invoke();

        // then log should contain changes for "all databases except..."
        var sortedRemainingMembers = memberIds( 1, 3, roleOffset );
        var expectedMessage = String.format( "%s for all databases except for %s is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             topologyDescription, namedDatabaseIds.first().databaseId(),
                                             sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldLogExplicitNamesForChangedDatabases( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 8 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when timer is fired after topology change for 2 out of 8 databases
        var changedDatabases = new ArrayList<DatabaseId>( 2 );
        namedDatabaseIds.stream().limit( 2 )
                                .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseId ) )
                                .forEach( pair -> {
                                    topologyLogger.logTopologyChange( topologyDescription, pair.first(), pair.other() );
                                    changedDatabases.add( pair.first().databaseId() );
                                } );
        timerService.getTimer().invoke();

        // then log should contain explicit names for changed databases
        var sortedRemainingMembers = memberIds( 1, 3, roleOffset );
        var expectedMessage = String.format( "%s for databases %s and %s is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             topologyDescription, changedDatabases.get( 0 ), changedDatabases.get( 1 ),
                                             sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "topologyTypes" )
    void shouldLogNumberOfDatabasesChanged( TopologyChangePair<?> topologyChangePair, int roleOffset, String topologyDescription, String testName )
    {
        // given
        final int NBR_CHANGED_DATABASES = 8;

        var namedDatabaseIds = generateNamedDatabaseIds( 15 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       memberIds( MEMBERS_PER_ROLE, 2 * MEMBERS_PER_ROLE ),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var logger = new AssertableLogProvider();
        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var topologyLogger = new TopologyLogger( timerService, logger, GlobalTopologyState.class, () -> databaseIds );

        // when timer is fired after topology change for 8 out of 15 databases
        namedDatabaseIds.stream().limit( NBR_CHANGED_DATABASES )
                        .map( namedDatabaseId -> topologyChangePair.create( topologyService, memberId( roleOffset ), namedDatabaseId ) )
                        .forEach( pair -> topologyLogger.logTopologyChange( topologyDescription, pair.first(), pair.other() ) );
        timerService.getTimer().invoke();

        // then log should contain number of databases changed
        var sortedRemainingMembers = memberIds( 1, 3, roleOffset );
        var expectedMessage = String.format( "%s for %d databases is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             topologyDescription, NBR_CHANGED_DATABASES, sortedRemainingMembers, Set.of( memberId( roleOffset ) ) );
        assertThat( logger )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void newBatchShouldResetTimeout()
    {
        // given
        var namedDatabaseIds = generateNamedDatabaseIds( 3 );
        var databaseIds = extractDatabaseIds( namedDatabaseIds );
        var topologyService = new FakeTopologyService( memberIds( 0, MEMBERS_PER_ROLE ),
                                                       Set.of(),
                                                       memberId( 0 ),
                                                       namedDatabaseIds );

        var scheduler = new FakeClockJobScheduler();
        var timerService = new StubTimerService( scheduler );
        var logProvider = new AssertableLogProvider();
        var batchTime = Duration.ofMillis( 1000 );
        var topologyLogger = new TopologyLogger( timerService, logProvider, GlobalTopologyState.class, () -> databaseIds, batchTime );

        var dbIds = namedDatabaseIds.toArray( NamedDatabaseId[]::new );
        var batch1 = coreChangeFactory.create( topologyService, memberId( 0 ), dbIds[0] );
        var batch2Change1 = coreChangeFactory.create( topologyService, memberId( 1 ), dbIds[1] );
        var batch2Change2 = coreChangeFactory.create( topologyService, memberId( 1 ), dbIds[2] );

        // when
        // First batch starts timer for batch time
        topologyLogger.logTopologyChange( CORE_TOPOLOGY_DESCRIPTION, batch1.first(), batch1.other() );
        // Half the batch time passes
        scheduler.forward( batchTime.dividedBy( 2 ) );
        // Second batch should reset timer and cause logging of first batch
        topologyLogger.logTopologyChange( CORE_TOPOLOGY_DESCRIPTION, batch2Change1.first(), batch2Change1.other() );

        // then
        var sortedRemainingMembers = topologyService.allCoreServers().keySet().stream()
                                                    .filter( member -> !Objects.equals( member, memberId( 0 ) ) )
                                                    .collect( sortedSetCollector( MemberId::getUuid ) );
        var expectedMessage = String.format( "%s for database %s is now: %s" +
                                             System.lineSeparator() +
                                             "Lost members :%s" +
                                             System.lineSeparator() +
                                             "No new members",
                                             CORE_TOPOLOGY_DESCRIPTION,
                                             dbIds[0].databaseId(),
                                             sortedRemainingMembers,
                                             Set.of( memberId( 0 ) ) );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
        logProvider.clear();

        // when
        // Additional log in second batch
        topologyLogger.logTopologyChange( CORE_TOPOLOGY_DESCRIPTION, batch2Change2.first(), batch2Change2.other() );
        // Second half (plus 1 milli) of original batch time passes and the internal timer elapses
        scheduler.forward( batchTime.dividedBy( 2 ).plusMillis( 1 ) );

        // then
        // Neither additional log in second batch, nor elapse of original timer should cause logging
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // when
        // Second half of batch time (after timer reset) passes and internal timer elapses again
        scheduler.forward( batchTime.dividedBy( 2 ) );

        //then
        sortedRemainingMembers = topologyService.allCoreServers().keySet().stream()
                                                .filter( member -> !Objects.equals( member, memberId( 1 ) ) )
                                                .collect( sortedSetCollector( MemberId::getUuid ) );
        expectedMessage = String.format( "%s for all databases except for %s is now: %s" +
                                         System.lineSeparator() +
                                         "Lost members :%s" +
                                         System.lineSeparator() +
                                         "No new members",
                                         CORE_TOPOLOGY_DESCRIPTION,
                                         dbIds[0].databaseId(),
                                         sortedRemainingMembers,
                                         Set.of( memberId( 1 ) ) );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @FunctionalInterface
    interface TopologyChangePair<T extends Topology<? extends DiscoveryServerInfo>>
    {
        Pair<T,T> create( TopologyService topologyService, MemberId removed, NamedDatabaseId database );
    }

    private static Pair<DatabaseCoreTopology,DatabaseCoreTopology> newOldCoreTopology( TopologyService topologyService, MemberId removed,
            NamedDatabaseId databaseId )
    {
        var oldTopology = topologyService.coreTopologyForDatabase( databaseId );

        var membersMinusRemoved = new HashMap<>( oldTopology.servers() );
        membersMinusRemoved.remove( removed );

        var newTopology = new DatabaseCoreTopology( databaseId.databaseId(), oldTopology.raftId(), membersMinusRemoved );
        return Pair.of( newTopology, oldTopology );
    }

    private static Pair<DatabaseReadReplicaTopology,DatabaseReadReplicaTopology> newOldReplicaTopology( TopologyService topologyService, MemberId removed,
            NamedDatabaseId databaseId )
    {
        var oldReplicaTopology = topologyService.readReplicaTopologyForDatabase( databaseId );

        var membersMinusRemoved = new HashMap<>( oldReplicaTopology.servers() );
        membersMinusRemoved.remove( removed );

        var newReplicaTopology = new DatabaseReadReplicaTopology( databaseId.databaseId(), membersMinusRemoved );
        return Pair.of( newReplicaTopology, oldReplicaTopology );
    }

    /* Note generators below produce sorted collections because we need stable orderings for asserting on log message contents */

    private static SortedSet<NamedDatabaseId> generateNamedDatabaseIds( int size )
    {
        return Stream.generate( TestDatabaseIdRepository::randomNamedDatabaseId )
                     .limit( size )
                     .collect( sortedSetCollector( namedDbId -> namedDbId.databaseId().uuid() ) );
    }

    private static SortedSet<DatabaseId> extractDatabaseIds( Set<NamedDatabaseId> namedDatabaseIds )
    {
        return namedDatabaseIds.stream().map( NamedDatabaseId::databaseId )
                               .collect( sortedSetCollector( DatabaseId::uuid ) );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static SortedSet<MemberId> memberIds( int from, int until, int roleOffset )
    {
        return memberIds( from + roleOffset, until + roleOffset );
    }

    private static SortedSet<MemberId> memberIds( int from, int until )
    {
        var result = new TreeSet<>( Comparator.comparing( MemberId::getUuid ) );
        result.addAll( FakeTopologyService.memberIds( from, until ) );
        return result;
    }

    static <T, U extends Comparable<? super U>> Collector<T,?,SortedSet<T>> sortedSetCollector( Function<? super T, ? extends U> comparing )
    {
        return Collectors.toCollection( () -> new TreeSet<>( Comparator.comparing( comparing ) ) );
    }

    private static class StubTimerService extends TimerService
    {
        private Timer singleton;

        StubTimerService( JobScheduler jobScheduler )
        {
            super( jobScheduler, NullLogProvider.getInstance() );
        }

        @Override
        public synchronized Timer create( TimerName name, Group group, TimeoutHandler handler )
        {
            singleton = spy( new StubTimer( scheduler, name, group, handler ) );
            return singleton;
        }

        @Override
        public synchronized Collection<Timer> getTimers( TimerName name )
        {
           throw new UnsupportedOperationException( "Its a stub" );
        }

        public Timer getTimer()
        {
            return singleton;
        }

        @Override
        public synchronized void invoke( TimerName name )
        {
            throw new UnsupportedOperationException( "Its a stub" );
        }
    }

    private static class StubTimer extends Timer
    {
        StubTimer( JobScheduler jobScheduler, TimerService.TimerName name, Group group, TimeoutHandler timeoutHandler )
        {
            super( name, jobScheduler, NullLog.getInstance(), group, timeoutHandler );
        }
    }
}
