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
import com.neo4j.causalclustering.messaging.Inbound;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class TransferLeaderTest
{
    private final MemberId myself = new MemberId( randomUUID() );
    private final MemberId core1 = new MemberId( randomUUID() );
    private final Set<NamedDatabaseId> databaseIds = Set.of( randomNamedDatabaseId() );
    private final RaftMembershipResolver raftMembershipResolver = new StubRaftMembershipResolver( myself, core1 );
    private final Config config = Config.defaults();
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final DatabasePenalties databasePenalties = new DatabasePenalties( 1, TimeUnit.MILLISECONDS, Clocks.fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader = new TransferLeader( config, messageHandler, myself, databasePenalties, new RandomStrategy(),
                                                           raftMembershipResolver, () -> myLeaderships );
        var databaseId = databaseIds.iterator().next();
        // I am leader
        myLeaderships.add( databaseId );
        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftId().uuid(), databaseId.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamNotLeader()
    {
        var myLeaderships = new ArrayList<>( databaseIds );
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                                    SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        var databaseId = databaseIds.iterator().next();
        // I am not leader
        myLeaderships.remove( databaseId );

        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInPrioritisedGroup()
    {
        var myLeaderships = new ArrayList<NamedDatabaseId>();
        TransferLeader transferLeader =
                new TransferLeader( config, messageHandler, myself, databasePenalties,
                                    SelectionStrategy.NO_OP, raftMembershipResolver, () -> myLeaderships );
        var databaseId = databaseIds.iterator().next();
        // I am leader
        myLeaderships.add( databaseId );
        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );
        // I am of group prio
        config.set( CausalClusteringSettings.server_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    // TODO add more tests!

    private static class StubRaftMembershipResolver implements RaftMembershipResolver
    {
        private final Set<MemberId> votingMembers;

        StubRaftMembershipResolver( MemberId... members )
        {
            this.votingMembers = Set.of( members );
        }

        @Override
        public RaftMembership membersFor( NamedDatabaseId namedDatabaseId )
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
