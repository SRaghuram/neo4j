/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeadershipTransferorTest
{
    private final ClusteringIdentityModule clusteringIdentityModule = new StubClusteringIdentityModule();
    private final MemberId myself = clusteringIdentityModule.memberId();
    private final MemberId other = IdFactory.randomMemberId();
    private final NamedDatabaseId fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private final ServerGroupName serverGroupName = new ServerGroupName( "prio" );

    @Test
    void shouldThrowIfNoGrupExistsForDatabase()
    {
        // given
        var leadershipTransferor = new LeadershipTransferor( message ->
        {
        }, clusteringIdentityModule, new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() ), new StubRaftMembershipResolver( myself, other ),
                Clocks.fakeClock() );

        // when there is no server id with the database
        Function<NamedDatabaseId,Set<ServerGroupName>> namedDatabaseIdSetFunction = dbid -> Set.of();

        // then this is wrong usage and should throw
        assertThrows( IllegalArgumentException.class,
                () ->
                {
                    leadershipTransferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), namedDatabaseIdSetFunction );
                } );
    }

    @Test
    void shouldReturnFalseIfThereIsNoTarget()
    {
        // given
        var leadershipTransferor = new LeadershipTransferor( message ->
        {
        }, clusteringIdentityModule, new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() ), new StubRaftMembershipResolver( myself, other ),
                Clocks.fakeClock() );

        // and
        var noOp = SelectionStrategy.NO_OP;

        // then
        assertFalse( leadershipTransferor.toPrioritisedGroup( List.of( fooId ), noOp, dbi -> Set.of( serverGroupName ) ) );

        // and
        assertFalse( leadershipTransferor.balanceLeadership( List.of( fooId ), noOp ) );
    }

    @Test
    void shouldSuccessfullySendToPriorityGroupIfExists()
    {
        // given
        AtomicReference<RaftMessages.InboundRaftMessageContainer<?>> raftMessage = new AtomicReference<>();
        var leadershipTransferor =
                new LeadershipTransferor( raftMessage::set, clusteringIdentityModule, new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() ),
                        new StubRaftMembershipResolver( myself, other ), Clocks.fakeClock() );

        // when
        var result = leadershipTransferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), dbi -> Set.of( serverGroupName ) );

        // then
        assertTrue( result );
        assertNotNull( raftMessage.get() );
        assertThat( raftMessage.get().message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myself, other, Set.of( serverGroupName ) ) );
    }

    @Test
    void shouldSuccessfullyBalance()
    {
        // given
        AtomicReference<RaftMessages.InboundRaftMessageContainer<?>> raftMessage = new AtomicReference<>();
        var leadershipTransferor =
                new LeadershipTransferor( raftMessage::set, clusteringIdentityModule, new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() ),
                        new StubRaftMembershipResolver( myself, other ), Clocks.fakeClock() );

        // when
        var result = leadershipTransferor.balanceLeadership( List.of( fooId ), new RandomStrategy() );

        // then
        assertTrue( result );
        assertNotNull( raftMessage.get() );
        assertThat( raftMessage.get().message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myself, other, Set.of() ) );
    }

    @Test
    void shouldNotBalanceIfPenalised()
    {
        // given
        AtomicReference<RaftMessages.InboundRaftMessageContainer<?>> raftMessage = new AtomicReference<>();
        var databasePenalties = new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() );
        var leadershipTransferor = new LeadershipTransferor( raftMessage::set, clusteringIdentityModule, databasePenalties,
                new StubRaftMembershipResolver( myself, other ), Clocks.fakeClock() );

        // and
        databasePenalties.issuePenalty( other, fooId );

        // when
        var result = leadershipTransferor.balanceLeadership( List.of( fooId ), new RandomStrategy() );

        // then
        assertFalse( result );
        assertNull( raftMessage.get() );
    }

    @Test
    void shouldNotSendToPenalisedPriortyGroupMember()
    {
        // given
        AtomicReference<RaftMessages.InboundRaftMessageContainer<?>> raftMessage = new AtomicReference<>();
        var databasePenalties = new DatabasePenalties( Duration.ofSeconds( 10 ), Clocks.fakeClock() );
        var leadershipTransferor = new LeadershipTransferor( raftMessage::set, clusteringIdentityModule, databasePenalties,
                new StubRaftMembershipResolver( myself, other ), Clocks.fakeClock() );

        // and
        databasePenalties.issuePenalty( other, fooId );

        // when
        var result = leadershipTransferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), dbid -> Set.of( serverGroupName ) );

        // then
        assertFalse( result );
        assertNull( raftMessage.get() );
    }
}
