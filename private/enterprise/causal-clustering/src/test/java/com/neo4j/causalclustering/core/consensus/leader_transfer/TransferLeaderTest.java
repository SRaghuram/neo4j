/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class TransferLeaderTest
{
    private final MemberId myself = new MemberId( randomUUID() );
    private final MemberId core1 = new MemberId( randomUUID() );
    private final NamedDatabaseId databaseId1 = randomNamedDatabaseId();
    private final NamedDatabaseId databaseId2 = randomNamedDatabaseId();
    private final RaftMembershipResolver raftMembershipResolver = new StubRaftMembershipResolver( myself, core1 );
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final DatabasePenalties databasePenalties = new DatabasePenalties( Duration.ofMillis( 1 ), Clocks.fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader = new TransferLeader( config, messageHandler, myself, databasePenalties, new RandomStrategy(),
                raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeader.run();

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

        // I am not leader for any database
        List<NamedDatabaseId> myLeaderships = List.of();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInPrioritisedGroup()
    {
        // Priority group exist and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldNotTransferIfPriorityGroupsIsEmpty()
    {
        // Priority group does not exist
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "" ) ).build();

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId1 );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldFallBackToNormalLoadBalancingWithNoGroupsIfNoTarget()
    {
        // Priority group exist for one db and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId1.name() ).setting().name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "prio" ) ).build();

        var transferee = core1;
        var selectionStrategyInputs = new ArrayList<TransferCandidates>();
        SelectionStrategy mockSelectionStrategy = validTopologies ->
        {
            selectionStrategyInputs.addAll( validTopologies );
            var transferCandidates = validTopologies.get(0);
            return new LeaderTransferTarget( transferCandidates.databaseId(), transferee );
        };

        var myLeaderships = List.of( databaseId1, databaseId2 );
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties, mockSelectionStrategy, raftMembershipResolver, () -> myLeaderships );
        // when
        transferLeader.run();

        // then
        assertThat( selectionStrategyInputs ).contains( new TransferCandidates( databaseId2, Set.of( transferee ) ) );
        assertThat( messageHandler.proposals.get( 0 ).message() ).isEqualTo( new RaftMessages.LeadershipTransfer.Proposal( myself, transferee, Set.of() ) );
    }

    @Test
    void shouldHandleMoreThanOneDatbaseInPro()
    {
        var databaseOne = DatabaseIdFactory.from( "one", randomUUID() );
        var databaseIds = Set.of( databaseOne, DatabaseIdFactory.from( "two", randomUUID() ) );
        // Priority groups exist ...
        var builder = Config.newBuilder();
        databaseIds.forEach( dbid -> builder.setRaw( Map.of( new LeadershipPriorityGroupSetting( dbid.name() ).setting().name(), dbid.name() ) ).build() );
        // ...and I am in one of them
        builder.set( CausalClusteringSettings.server_groups, ServerGroupName.listOf( "two" ) );
        var config = builder.build();

        var myLeaderships = new ArrayList<>( databaseIds );
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );

        // when
        transferLeader.run();

        // then we should propose new leader for 'one'
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftId().uuid(), databaseOne.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
        assertEquals( propose.message().priorityGroups(), ServerGroupName.setOf( "one" ) );
    }

    private static class StubRaftMembershipResolver implements RaftMembershipResolver
    {
        private final Set<MemberId> votingMembers;

        StubRaftMembershipResolver( MemberId... members )
        {
            this.votingMembers = Set.of( members );
        }

        @Override
        public RaftMembership membersFor( NamedDatabaseId databaseId )
        {
            return new StubRaftMembership( votingMembers );
        }
    }

    private static class StubRaftMembership implements RaftMembership
    {
        private final Set<MemberId> memberIds;

        StubRaftMembership( Set<MemberId> memberIds )
        {
            this.memberIds = memberIds;
        }

        @Override
        public Set<MemberId> votingMembers()
        {
            return memberIds;
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return memberIds;
        }

        @Override
        public void registerListener( Listener listener )
        { // no-op
        }
    }

    private static class TrackingMessageHandler implements Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
    {
        private final ArrayList<RaftMessages.InboundRaftMessageContainer<RaftMessages.LeadershipTransfer.Proposal>> proposals = new ArrayList<>();

        @Override
        public void handle( RaftMessages.InboundRaftMessageContainer<?> message )
        {
            if ( message.message() instanceof RaftMessages.LeadershipTransfer.Proposal )
            {
                proposals.add( (RaftMessages.InboundRaftMessageContainer<RaftMessages.LeadershipTransfer.Proposal>) message );
            }
            else
            {
                throw new IllegalArgumentException( "Unexpected message type " + message );
            }
        }
    }
}
