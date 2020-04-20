/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private final Set<NamedDatabaseId> databaseIds = Set.of( randomNamedDatabaseId() );
    private final RaftMembershipResolver raftMembershipResolver = new StubRaftMembershipResolver( myself, core1 );
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final DatabasePenalties databasePenalties = new DatabasePenalties( 1, TimeUnit.MILLISECONDS, Clocks.fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        var databaseId = databaseIds.iterator().next();
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId.name() ).setting().name(), "prio" ) ).build();
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader = new TransferLeader( config, messageHandler, myself, databasePenalties, new RandomStrategy(),
                raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId );

        // when
        transferLeader.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftId().uuid(), databaseId.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
        assertEquals( propose.message().priorityGroups(), Set.of( "prio" ) );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamNotLeader()
    {
        var databaseId = databaseIds.iterator().next();
        // Priority group exist and I am not in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId.name() ).setting().name(), "prio" ) ).build();

        var myLeaderships = new ArrayList<>( databaseIds );
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        // I am not leader
        myLeaderships.remove( databaseId );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInPrioritisedGroup()
    {
        var databaseId = databaseIds.iterator().next();
        // Priority group exist and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId.name() ).setting().name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, List.of( "prio" ) ).build();

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldNotTransferIfPriorityGroupsIsEmpty()
    {
        var databaseId = databaseIds.iterator().next();
        // Priority group does not exist
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId.name() ).setting().name(), "" ) ).build();

        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldFallBackToNormalLoadBalancingWithNoGroupsIfNoTarget()
    {
        var databaseId = databaseIds.iterator().next();
        // Priority group exist and I am in it
        var config = Config.newBuilder().setRaw( Map.of( new LeadershipPriorityGroupSetting( databaseId.name() ).setting().name(), "prio" ) )
                .set( CausalClusteringSettings.server_groups, List.of( "prio" ) ).build();

        var loadaBalancerLTC = new LeaderTransferContext( databaseId, RaftId.from( databaseId.databaseId() ), new MemberId( UUID.randomUUID() ) );
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                        validTopologies -> loadaBalancerLTC, raftMembershipResolver, () -> myLeaderships );
        // I am leader
        myLeaderships.add( databaseId );
        // when
        transferLeader.run();

        // then
        assertThat( messageHandler.proposals.get( 0 ).message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myself, loadaBalancerLTC.to(), Set.of() ) );
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
        builder.set( CausalClusteringSettings.server_groups, List.of( "two" ) );
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
        assertEquals( propose.message().priorityGroups(), Set.of( "one" ) );
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
        private Set<MemberId> memberIds;

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
