/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.time.Clocks.fakeClock;

class LeadershipTransferorTest
{
    private final CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity();
    private final CoreServerIdentity other = new InMemoryCoreServerIdentity();
    private final NamedDatabaseId fooId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private final ServerGroupName serverGroupName = new ServerGroupName( "prio" );

    private final AtomicReference<RaftMessages.InboundRaftMessageContainer<?>> raftMessage = new AtomicReference<>();

    private StubRaftMembershipResolver membershipResolver;

    private DatabasePenalties databasePenalties;
    private LeadershipTransferor transferor;

    @BeforeEach
    public void setup()
    {
        membershipResolver = new StubRaftMembershipResolver( fooId, myIdentity, other );
        databasePenalties = new DatabasePenalties( Duration.ofSeconds( 10 ), fakeClock() );
        transferor = new LeadershipTransferor( raftMessage::set, myIdentity, databasePenalties, membershipResolver, fakeClock() );
    }

    @Test
    void shouldThrowIfNoGroupExistsForDatabase()
    {
        // when there is no server id with the database
        Function<NamedDatabaseId,Set<ServerGroupName>> namedDatabaseIdSetFunction = dbid -> Set.of();

        // then this is wrong usage and should throw
        assertThrows( IllegalArgumentException.class, () ->
        {
            transferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), namedDatabaseIdSetFunction );
        } );
    }

    @Test
    void shouldReturnFalseIfThereIsNoTarget()
    {
        // given
        var transferor = new LeadershipTransferor( message -> { }, myIdentity, databasePenalties, membershipResolver, fakeClock() );

        // and
        var noOp = SelectionStrategy.NO_OP;

        // then
        assertFalse( transferor.toPrioritisedGroup( List.of( fooId ), noOp, dbi -> Set.of( serverGroupName ) ) );

        // and
        assertFalse( transferor.balanceLeadership( List.of( fooId ), noOp ) );
    }

    @Test
    void shouldSuccessfullySendToPriorityGroupIfExists()
    {
        // when
        var result = transferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), dbi -> Set.of( serverGroupName ) );

        // then
        assertTrue( result );
        assertNotNull( raftMessage.get() );
        assertThat( raftMessage.get().message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myIdentity.raftMemberId( fooId ), other.raftMemberId( fooId ), Set.of( serverGroupName ) ) );
    }

    @Test
    void shouldSuccessfullyBalance()
    {
        // when
        var result = transferor.balanceLeadership( List.of( fooId ), new RandomStrategy() );

        // then
        assertTrue( result );
        assertNotNull( raftMessage.get() );
        assertThat( raftMessage.get().message() ).isEqualTo(
                new RaftMessages.LeadershipTransfer.Proposal( myIdentity.raftMemberId( fooId ), other.raftMemberId( fooId ), Set.of() ) );
    }

    @Test
    void shouldNotBalanceIfPenalised()
    {
        // given
        databasePenalties.issuePenalty( other.serverId(), fooId );

        // when
        var result = transferor.balanceLeadership( List.of( fooId ), new RandomStrategy() );

        // then
        assertFalse( result );
        assertNull( raftMessage.get() );
    }

    @Test
    void shouldNotSendToPenalisedPriorityGroupMember()
    {
        // given
        databasePenalties.issuePenalty( other.serverId(), fooId );

        // when
        var result = transferor.toPrioritisedGroup( List.of( fooId ), new RandomStrategy(), dbid -> Set.of( serverGroupName ) );

        // then
        assertFalse( result );
        assertNull( raftMessage.get() );
    }
}
