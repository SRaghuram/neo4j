/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.io.marshal.ChannelMarshal;

import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class RaftMembershipStateTest implements BaseMarshalTest<RaftMembershipState>
{
    private RaftMembershipState state = new RaftMembershipState();

    private Set<RaftMemberId> membersA = asSet( raftMember( 0 ), raftMember( 1 ), raftMember( 2 ) );
    private Set<RaftMemberId> membersB = asSet( raftMember( 0 ), raftMember( 1 ), raftMember( 2 ), raftMember( 3 ) );

    @Override
    public Collection<RaftMembershipState> originals()
    {
        return List.of( new RaftMembershipState( 5, new MembershipEntry( 7, membersA ), new MembershipEntry( 8, membersB ) ) );
    }

    @Override
    public ChannelMarshal<RaftMembershipState> marshal()
    {
        return new RaftMembershipState.Marshal();
    }

    @Test
    void shouldHaveCorrectInitialState()
    {
        assertThat( state.getLatest(), hasSize( 0 ) );
        Assertions.assertFalse( state.uncommittedMemberChangeInLog() );
    }

    @Test
    void shouldUpdateLatestOnAppend()
    {
        // when
        state.append( 0, membersA );

        // then
        Assertions.assertEquals( state.getLatest(), membersA );

        // when
        state.append( 1, membersB );

        // then
        Assertions.assertEquals( state.getLatest(), membersB );
        Assertions.assertEquals( 1, state.getOrdinal() );
    }

    @Test
    void shouldKeepLatestOnCommit()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );

        // when
        state.commit( 0 );

        // then
        Assertions.assertEquals( state.getLatest(), membersB );
        Assertions.assertTrue( state.uncommittedMemberChangeInLog() );
        Assertions.assertEquals( 1, state.getOrdinal() );
    }

    @Test
    void shouldLowerUncommittedFlagOnCommit()
    {
        // given
        state.append( 0, membersA );
        Assertions.assertTrue( state.uncommittedMemberChangeInLog() );

        // when
        state.commit( 0 );

        // then
        Assertions.assertFalse( state.uncommittedMemberChangeInLog() );
    }

    @Test
    void shouldRevertToCommittedStateOnTruncation()
    {
        // given
        state.append( 0, membersA );
        state.commit( 0 );
        state.append( 1, membersB );
        Assertions.assertEquals( state.getLatest(), membersB );

        // when
        state.truncate( 1 );

        // then
        Assertions.assertEquals( state.getLatest(), membersA );
        Assertions.assertEquals( 3, state.getOrdinal() );
    }

    @Test
    void shouldNotTruncateEarlierThanIndicated()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );
        Assertions.assertEquals( state.getLatest(), membersB );

        // when
        state.truncate( 2 );

        // then
        Assertions.assertEquals( state.getLatest(), membersB );
        Assertions.assertEquals( 1, state.getOrdinal() );
    }

    @Test
    void shouldRefuseToAppendToTheSameIndexTwice()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );

        // when
        boolean reAppendA = state.append( 0, membersA );
        boolean reAppendB = state.append( 1, membersB );

        // then
        Assertions.assertFalse( reAppendA );
        Assertions.assertFalse( reAppendB );
        Assertions.assertEquals( membersA, state.committed().members() );
        Assertions.assertEquals( membersB, state.getLatest() );
    }
}
