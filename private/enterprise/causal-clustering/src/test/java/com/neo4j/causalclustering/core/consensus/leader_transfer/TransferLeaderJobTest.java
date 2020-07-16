/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.configuration.ServerGroupsSupplier.listen;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.time.Clocks.fakeClock;

class TransferLeaderJobTest
{
    private final ClusteringIdentityModule identityModule = new StubClusteringIdentityModule();
    private final MemberId myself = identityModule.memberId();
    private final ClusteringIdentityModule remoteIdentityModule = new StubClusteringIdentityModule();
    private final MemberId core1 = remoteIdentityModule.memberId();
    private final NamedDatabaseId databaseId1 = randomNamedDatabaseId();
    private final NamedDatabaseId databaseId2 = randomNamedDatabaseId();
    private final RaftMembershipResolver raftMembershipResolver = new StubRaftMembershipResolver( myself, core1 );
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final DatabasePenalties databasePenalties = new DatabasePenalties( Duration.ofMillis( 1 ), fakeClock() );
    private final LeadershipTransferor leadershipTransferor =
            new LeadershipTransferor( messageHandler, identityModule, databasePenalties, raftMembershipResolver, fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) ).build();
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
        assertEquals( propose.raftId().uuid(), databaseId1.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
        assertEquals( propose.message().priorityGroups(), Set.of( new ServerGroupName( "prio" ) ) );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamNotLeader()
    {
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) ).build();
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
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) )
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
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "" ) ).build();
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
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();
        var serverGroupsSupplier = listen( config );

        var transferee = core1;
        var selectionStrategyInputs = new ArrayList<TransferCandidates>();
        SelectionStrategy mockSelectionStrategy = validTopologies ->
        {
            selectionStrategyInputs.addAll( validTopologies );
            var transferCandidates = validTopologies.get( 0 );
            return new LeaderTransferTarget( transferCandidates.databaseId(), transferee );
        };

        var myLeaderships = List.of( databaseId1, databaseId2 );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, mockSelectionStrategy, () -> myLeaderships );
        // when
        transferLeaderJob.run();

        // then
        assertThat( selectionStrategyInputs ).contains( new TransferCandidates( databaseId2, Set.of( transferee ) ) );
        assertThat( messageHandler.proposals.get( 0 ).message() ).isEqualTo( new RaftMessages.LeadershipTransfer.Proposal( myself, transferee, Set.of() ) );
    }

    @Test
    void shouldNotDoNormalLoadBalancingForSystemDatabase()
    {
        // given
        var transferee = core1;
        var selectionStrategyInputs = new ArrayList<TransferCandidates>();
        SelectionStrategy mockSelectionStrategy = validTopologies ->
        {
            selectionStrategyInputs.addAll( validTopologies );
            var transferCandidates = validTopologies.get( 0 );
            return new LeaderTransferTarget( transferCandidates.databaseId(), transferee );
        };
        var nonSystemLeaderships = List.of( databaseId1 );

        var config = Config.defaults();
        var serverGroupsSupplier = listen( config );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, mockSelectionStrategy, () -> nonSystemLeaderships );

        // when
        transferLeaderJob.run();

        // then
        assertThat( selectionStrategyInputs ).contains( new TransferCandidates( databaseId1, Set.of( transferee ) ) );

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
    void shouldHandleMoreThanOneDatbaseInPrio()
    {
        var databaseOne = DatabaseIdFactory.from( "one", randomUUID() );
        var databaseIds = Set.of( databaseOne, DatabaseIdFactory.from( "two", randomUUID() ) );
        // Priority groups exist ...
        var builder = Config.newBuilder();
        databaseIds.forEach( dbid -> builder.setRaw( Map.of( new LeadershipPriorityGroupSetting( dbid.name() ).setting().name(), dbid.name() ) ).build() );
        // ...and I am in one of them
        builder.set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "two" ) );
        var config = builder.build();
        var serverGroupsSupplier = listen( config );

        var myLeaderships = new ArrayList<>( databaseIds );
        var transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config, SelectionStrategy.NO_OP, () -> myLeaderships );

        // when
        transferLeaderJob.run();

        // then we should propose new leader for 'one'
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftId().uuid(), databaseOne.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
        assertEquals( propose.message().priorityGroups(), ServerGroupName.setOf( "one" ) );
    }

    @Test
    void shouldAdaptToDynamicChangesInMyServerGroups()
    {
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) )
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
                new RaftMessages.LeadershipTransfer.Proposal( myself, core1, Set.of( new ServerGroupName( "prio" ) ) ) );
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
}
