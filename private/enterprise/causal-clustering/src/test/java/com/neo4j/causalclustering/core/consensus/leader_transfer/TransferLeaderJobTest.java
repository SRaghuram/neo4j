/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.configuration.ServerGroupsSupplier.listen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.time.Clocks.fakeClock;

class TransferLeaderJobTest
{
    private final CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity( 0 );
    private final CoreServerIdentity remoteIdentity = new InMemoryCoreServerIdentity( 1 );
    private final NamedDatabaseId databaseId1 = randomNamedDatabaseId();
    private final NamedDatabaseId databaseId2 = randomNamedDatabaseId();
    private final RaftMembershipResolver raftMembershipResolver = new StubRaftMembershipResolver()
            .add( databaseId1, myIdentity, remoteIdentity )
            .add( databaseId2, myIdentity, remoteIdentity );
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final DatabasePenalties databasePenalties = new DatabasePenalties( Duration.ofMillis( 1 ), fakeClock() );
    private final LeadershipTransferor leadershipTransferor =
            new LeadershipTransferor( messageHandler, myIdentity, databasePenalties, raftMembershipResolver, fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "prio" ) )
                           .build();
        var serverGroupsSupplier = listen( config );
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, new RandomStrategy(), () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftGroupId().uuid(), databaseId1.databaseId().uuid() );
        assertEquals( propose.message().proposed(), remoteIdentity.raftMemberId( databaseId1 ) );
        assertEquals( propose.message().priorityGroups(), Set.of( new ServerGroupName( "prio" ) ) );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamNotLeader()
    {
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "prio" ) )
                           .build();
        var serverGroupsSupplier = listen( config );

        // I am not leader for any database
        List<NamedDatabaseId> myLeaderships = List.of();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        transferLeaderJob.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInPrioritisedGroup()
    {
        // Priority group exist and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "prio" ) )
                           .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldNotTransferIfPriorityGroupsIsEmptyAndStrategyIsNoOp()
    {
        // Priority group does not exist
        var config =
                Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "" ) ).build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldFallBackToNormalLoadBalancingWithNoGroupsIfNoTarget()
    {
        // Priority group exist for one db and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();
        var serverGroupsSupplier = listen( config );

        var selectionStrategyInputs = new ArrayList<TransferCandidates>();
        SelectionStrategy mockSelectionStrategy = validTopologies ->
        {
            selectionStrategyInputs.addAll( validTopologies );
            var transferCandidates = validTopologies.get( 0 );
            return new LeaderTransferTarget( transferCandidates.databaseId(), remoteIdentity.serverId() );
        };

        var myLeaderships = List.of( databaseId1, databaseId2 );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, mockSelectionStrategy, () -> myLeaderships );
        // when
        transferLeaderJob.run();

        // then
        assertThat( selectionStrategyInputs ).contains( new TransferCandidates( databaseId2, Set.of( remoteIdentity.serverId() ) ) );
        assertThat( messageHandler.proposals.get( 0 ).message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myIdentity.raftMemberId( databaseId2 ), remoteIdentity.raftMemberId( databaseId2 ), Set.of() ) );
    }

    @Test
    void shouldNotDoNormalLoadBalancingForSystemDatabase()
    {
        // given
        var transferee = remoteIdentity;
        var selectionStrategyInputs = new ArrayList<TransferCandidates>();
        SelectionStrategy mockSelectionStrategy = validTopologies ->
        {
            selectionStrategyInputs.addAll( validTopologies );
            var transferCandidates = validTopologies.get( 0 );
            return new LeaderTransferTarget( transferCandidates.databaseId(), transferee.serverId() );
        };
        var nonSystemLeaderships = List.of( databaseId1 );

        var config = Config.defaults();
        var serverGroupsSupplier = listen( config );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, mockSelectionStrategy, () -> nonSystemLeaderships );

        // when
        transferLeaderJob.run();

        // then
        assertThat( selectionStrategyInputs ).contains( new TransferCandidates( databaseId1, Set.of( transferee.serverId() ) ) );

        // given
        selectionStrategyInputs.clear();
        var systemLeaderships = List.of( NAMED_SYSTEM_DATABASE_ID );
        transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, mockSelectionStrategy, () -> systemLeaderships );

        // when
        transferLeaderJob.run();

        // then
        assertThat( selectionStrategyInputs ).isEmpty();
    }

    @Test
    void shouldHandleMoreThanOneDatabaseInPrio()
    {
        var groupOne = databaseId1.name();
        var groupTwo = databaseId2.name();
        var databaseIds = Set.of( databaseId1, databaseId2 );
        // Priority groups exist ...
        var builder = Config.newBuilder();
        databaseIds.forEach(
                dbId -> builder.setRaw( Map.of( new LeadershipPriorityGroupSetting( dbId.name() ).leadership_priority_group.name(), dbId.name() ) ).build() );
        // ...and I am in one of them
        builder.set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( groupTwo ) );
        var config = builder.build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<>( databaseIds );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        transferLeaderJob.run();

        // then we should propose new leader for groupOne
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftGroupId().uuid(), databaseId1.databaseId().uuid() );
        assertEquals( propose.message().proposed(), remoteIdentity.raftMemberId( databaseId1 ) );
        assertEquals( propose.message().priorityGroups(), ServerGroupName.setOf( groupOne ) );
    }

    @Test
    void shouldAdaptToDynamicChangesInMyServerGroups()
    {
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group.name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) )
                .build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = List.of( databaseId1 );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );
        // when I am leader in prio
        transferLeaderJob.run();

        // then I should remain leader
        assertTrue( messageHandler.proposals.isEmpty() );

        // when I am no longer member of prio
        config.setDynamic( CausalClusteringSettings.server_groups, ServerGroupName.listOf(), getClass().getSimpleName() );

        // and
        transferLeaderJob.run();

        // then I should try and pass on leadership
        assertThat( messageHandler.proposals.get( 0 ).message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myIdentity.raftMemberId( databaseId1 ), remoteIdentity.raftMemberId( databaseId1 ),
                        Set.of( new ServerGroupName( "prio" ) ) ) );
    }

    @Test
    void shouldChooseToTransferIfIAmNotInDefaultPriority()
    {
        // Default priority group exist and I am not in it
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, new ServerGroupName( "prio" ) ).build();
        var serverGroupsSupplier = listen( config );
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, new RandomStrategy(), () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftGroupId().uuid(), databaseId1.databaseId().uuid() );
        assertEquals( propose.message().proposed(), remoteIdentity.raftMemberId( databaseId1 ) );
        assertEquals( propose.message().priorityGroups(), Set.of( new ServerGroupName( "prio" ) ) );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInDefaultPrioritisedGroup()
    {
        // Default priority group exist and I am in it
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, new ServerGroupName( "prio" ) )
                           .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseExplicitPriorityGroupBeforeDefaultPriorityGroup()
    {
        // Default priority group exist and I am in it
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, new ServerGroupName( "default_prio" ) )
                           .set( Map.of(
                                   new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group,
                                   new ServerGroupName( "explicit_prio" )
                           ) )
                           .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "explicit_prio" ) ).build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeaderJob.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );

        // I am not in explicit_prio group
        config.setDynamic( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "default_prio" ), getClass().getName() );

        // when
        transferLeaderJob.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftGroupId().uuid(), databaseId1.databaseId().uuid() );
        assertEquals( propose.message().proposed(), remoteIdentity.raftMemberId( databaseId1 ) );
        assertEquals( propose.message().priorityGroups(), Set.of( new ServerGroupName( "explicit_prio" ) ) );
    }

    @Test
    void shouldReturnAllDatabasesWithDefaultGroup()
    {
        // Default priority leadership group is set but no explicit leadership groups are set
        var defaultGroupName = new ServerGroupName( "default_prio" );
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, defaultGroupName )
                           .set(
                                   new LeadershipPriorityGroupSetting( randomNamedDatabaseId().name() ).leadership_priority_group,
                                   new ServerGroupName( "other_group" )
                           ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var serverGroupsSupplier = listen( config );
        var stubLeadershipTransferor = new StubLeadershipTransferor();
        var transferLeaderJob = new TransferLeaderJob( stubLeadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        myLeaderships.add( databaseId1 );
        myLeaderships.add( databaseId2 );
        transferLeaderJob.run();
        var prioritisedGroups = stubLeadershipTransferor.prioritisedGroups;

        // then
        assertThat( prioritisedGroups.size() ).isEqualTo( myLeaderships.size() );
        prioritisedGroups.keySet().stream().forEach( namedDatabaseId -> {
            assertTrue( myLeaderships.contains( namedDatabaseId ) );
        } );
        prioritisedGroups.values().stream().forEach( serverGroupName -> {
            assertTrue( serverGroupName.equals( defaultGroupName ) );
        } );
    }

    @Test
    void shouldReturnEmptyMap()
    {
        // No priority leadership group is set. Neither default nor explicit.
        var config = Config.newBuilder()
                           .set(
                                   new LeadershipPriorityGroupSetting( randomNamedDatabaseId().name() ).leadership_priority_group,
                                   new ServerGroupName( "other_group" )
                           ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var serverGroupsSupplier = listen( config );
        var stubLeadershipTransferor = new StubLeadershipTransferor();
        var transferLeaderJob = new TransferLeaderJob( stubLeadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        myLeaderships.add( databaseId1 );
        myLeaderships.add( databaseId2 );
        transferLeaderJob.run();
        var prioritisedGroups = stubLeadershipTransferor.prioritisedGroups;

        // then
        assertTrue( prioritisedGroups.isEmpty() );
    }

    @Test
    void shouldReturnAllDatabasesButOne()
    {
        // Default priority leadership group is set. One database has priority leadership group explicit set to nothing.
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, new ServerGroupName( "default_prio" ) )
                           .set( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group, ServerGroupName.EMPTY )
                           .set(
                                   new LeadershipPriorityGroupSetting( randomNamedDatabaseId().name() ).leadership_priority_group,
                                   new ServerGroupName( "other_group" )
                           ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var serverGroupsSupplier = listen( config );
        var stubLeadershipTransferor = new StubLeadershipTransferor();
        var transferLeaderJob = new TransferLeaderJob( stubLeadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        myLeaderships.add( databaseId1 );
        myLeaderships.add( databaseId2 );
        transferLeaderJob.run();
        var prioritisedGroups = stubLeadershipTransferor.prioritisedGroups;

        // then
        assertThat( prioritisedGroups.size() ).isEqualTo( 1 );
        assertTrue( prioritisedGroups.containsKey( databaseId2 ) );
    }

    @Test
    void shouldReturnAllDatabasesWithExplicitGroup()
    {
        // Default priority leadership group is set and all databases has been explicit set.
        var defaultGroupName = new ServerGroupName( "default_prio" );
        var explicitGroupNames = Map.of( databaseId1, new ServerGroupName( "explicit_prio_1" ),
                                         databaseId2, new ServerGroupName( "explicit_prio_2" ));
        var config = Config.newBuilder().set( CausalClusteringSettings.default_leadership_priority_group, defaultGroupName )
                           .set( new LeadershipPriorityGroupSetting( databaseId1.name() ).leadership_priority_group, explicitGroupNames.get( databaseId1 ) )
                           .set( new LeadershipPriorityGroupSetting( databaseId2.name() ).leadership_priority_group, explicitGroupNames.get( databaseId2 ) )
                           .set(
                                   new LeadershipPriorityGroupSetting( randomNamedDatabaseId().name() ).leadership_priority_group,
                                   new ServerGroupName( "other_group" )
                           ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        var serverGroupsSupplier = listen( config );
        var stubLeadershipTransferor = new StubLeadershipTransferor();
        var transferLeaderJob = new TransferLeaderJob( stubLeadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        myLeaderships.add( databaseId1 );
        myLeaderships.add( databaseId2 );
        transferLeaderJob.run();
        var prioritisedGroups = stubLeadershipTransferor.prioritisedGroups;

        // then
        assertThat( prioritisedGroups.size() ).isEqualTo( myLeaderships.size() );
        for ( var entry: prioritisedGroups.entrySet() )
        {
            assertTrue( entry.getValue().equals( explicitGroupNames.get( entry.getKey() ) ) );
        }
    }

    static class TrackingMessageHandler implements Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
    {
        final ArrayList<RaftMessages.InboundRaftMessageContainer<RaftMessages.LeadershipTransfer.Proposal>> proposals = new ArrayList<>();
        final ArrayList<RaftMessages.InboundRaftMessageContainer<?>> others = new ArrayList<>();

        @Override
        public void handle( RaftMessages.InboundRaftMessageContainer<?> message )
        {
            if ( message.message() instanceof RaftMessages.LeadershipTransfer.Proposal )
            {
                proposals.add( (RaftMessages.InboundRaftMessageContainer<RaftMessages.LeadershipTransfer.Proposal>) message );
            }
            else
            {
                others.add( message );
                throw new IllegalArgumentException( "Unexpected message type " + message );
            }
        }
    }

    static class StubLeadershipTransferor extends LeadershipTransferor
    {
        Map<NamedDatabaseId, ServerGroupName> prioritisedGroups;

        StubLeadershipTransferor()
        {
            super( null, null, null, null, null );
        }

        @Override
        boolean toPrioritisedGroup( Collection<NamedDatabaseId> undesiredLeaders, SelectionStrategy selectionStrategy,
                                    Function<NamedDatabaseId,Set<ServerGroupName>> prioritisedGroupsProvider )
        {
            prioritisedGroups = undesiredLeaders.stream().collect( Collectors.toMap( Function.identity(),
                                               db -> prioritisedGroupsProvider.apply( db ).stream().findFirst().orElse( null ) ) );
            return true;
        }
    }
}
