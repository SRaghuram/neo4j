/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverIds;
import static com.neo4j.causalclustering.discovery.akka.BatchingMultiDatabaseLogger.newPaddedLine;
import static java.lang.System.lineSeparator;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class DatabaseStateLoggerTest
{
    private static final String DESCRIPTION = "State logger test";
    private static final int CHANGED_MEMBER_SEED = 0;

    @Test
    void equalStatesShouldReturnEmpty()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var dbId = randomDatabaseId();
        var newOldState1 = newOldCoreState( dbId );
        var newOldState2 = newOldCoreState( dbId );

        // when new and old states contain same info
        var result = stateLogger.computeChange( DESCRIPTION, newOldState1.first(), newOldState2.first() );

        // then should return empty optional
        assertThat( result.isEmpty() ).isTrue();
    }

    @Test
    void shouldNotLogNewMember()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var dbId = randomDatabaseId();
        var newOldState = newOldCoreState( dbId );
        var newMemberState = newOldState.first().memberStates().get( serverId( CHANGED_MEMBER_SEED ) );

        // when new state has an additional member
        var result = stateLogger.computeChange( DESCRIPTION, newOldState.first(), newOldState.other() );

        // then should return empty optional
        assertThat( result.isEmpty() ).isTrue();
    }

    @Test
    void shouldNotLogLostMember()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var dbId = randomDatabaseId();
        var newOldState = newOldCoreState( dbId );
        var lostMember = serverId( CHANGED_MEMBER_SEED );

        // when new state has lost a member
        var result = stateLogger.computeChange( DESCRIPTION, newOldState.other(), newOldState.first() );

        // then should return empty optional
        assertThat( result.isEmpty() ).isTrue();
    }

    @Test
    void shouldReturnCorrectSpecificationUpdatedMember()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var dbId = randomDatabaseId();
        var updatedMember = serverId( CHANGED_MEMBER_SEED );
        var newState = new DiscoveryDatabaseState( dbId, EnterpriseOperatorState.STARTED, null );

        var oldDatabaseStates = ReplicatedDatabaseState.ofCores( dbId, serverIds( 0, 3 ).stream()
                                                                                        .collect( Collectors.toMap( identity(), unused -> DiscoveryDatabaseState
                                                                                                .unknown( dbId ) ) ) );
        var newDatabaseStates = ReplicatedDatabaseState.ofCores( dbId, serverIds( 0, 3 ).stream()
                                                                                        .collect( Collectors.toMap( identity(),
                                                                                                                    memberId -> memberId.equals( updatedMember )
                                                                                                                                ?
                                                                                                                                newState :
                                                                                                                                DiscoveryDatabaseState
                                                                                                                                        .unknown( dbId ) ) ) );

        // when a member has an updated state
        var result = stateLogger.computeChange( DESCRIPTION, newDatabaseStates, oldDatabaseStates );

        // then should print the member with its change
        assertThat( result.isPresent() ).isTrue();
        var expectedSpecification = "changed" + lineSeparator() +
                                    "Servers changed their state:" + newPaddedLine() +
                                    "  to: " + updatedMember + "=" + newState + newPaddedLine() +
                                    "from: " + updatedMember + "=" + oldDatabaseStates.memberStates().get( updatedMember );
        assertThat( result.get().specification() ).isEqualTo( expectedSpecification );
    }

    @Test
    void shouldReturnCorrectTitle()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var dbId = randomDatabaseId();
        var updatedMember = serverId( CHANGED_MEMBER_SEED );
        var newState = new DiscoveryDatabaseState( dbId, EnterpriseOperatorState.STARTED, null );

        var oldDatabaseStates = ReplicatedDatabaseState
                .ofCores( dbId, serverIds( 0, 3 )
                        .stream()
                        .collect( Collectors.toMap( identity(), unused -> DiscoveryDatabaseState.unknown( dbId ) ) ) );
        var newDatabaseStates = ReplicatedDatabaseState
                .ofCores( dbId, serverIds( 0, 3 )
                        .stream()
                        .collect( Collectors.toMap( identity(), memberId -> memberId.equals( updatedMember ) ?
                                                                            newState :
                                                                            DiscoveryDatabaseState.unknown( dbId ) ) ) );

        // when a member has an updated state
        var result = stateLogger.computeChange( DESCRIPTION, newDatabaseStates, oldDatabaseStates );

        // then should return ChangeKey in an Optional, with correct title
        assertThat( result.isPresent() ).isTrue();
        assertThat( result.get().title() ).isEqualTo( DESCRIPTION );
    }

    @Test
    void shouldReturnDatabaseId()
    {
        // setup
        var timerService = new TimerService( new JobSchedulerAdapter(), NullLogProvider.getInstance() );
        var stateLogger = new DatabaseStateLogger( timerService, NullLogProvider.getInstance(), GlobalTopologyState.class, Set::of );

        // given
        var coreDbIdIn = randomDatabaseId();
        var rrDbIdIn = randomDatabaseId();

        var coreState = ReplicatedDatabaseState.ofCores( coreDbIdIn, Map.of() );
        var rrState = ReplicatedDatabaseState.ofReadReplicas( rrDbIdIn, Map.of() );

        // when
        var coreDbIdOut = stateLogger.extractDatabaseId( coreState );
        var rrDbIdOut = stateLogger.extractDatabaseId( rrState );

        // then
        assertThat( coreDbIdOut ).isEqualTo( coreDbIdIn );
        assertThat( rrDbIdOut ).isEqualTo( rrDbIdIn );
    }

    private static Pair<ReplicatedDatabaseState,ReplicatedDatabaseState> newOldCoreState( DatabaseId dbId )
    {
        Map<ServerId,DiscoveryDatabaseState> memberStates = serverIds( 0, 3 ).stream()
                                                                             .collect( Collectors.toMap( identity(),
                                                                                                         unused -> DiscoveryDatabaseState.unknown( dbId ) ) );

        var largerCoreState = ReplicatedDatabaseState.ofCores( dbId, Map.copyOf( memberStates ) );

        memberStates.remove( serverId( CHANGED_MEMBER_SEED ) );
        var smallerCoreState = ReplicatedDatabaseState.ofCores( dbId, Map.copyOf( memberStates ) );

        return Pair.of( largerCoreState, smallerCoreState );
    }
}
