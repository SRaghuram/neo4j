/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.ElectionTimerMode.ACTIVE_ELECTION;
import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder.builder;
import static com.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.Collections.emptySet;

class RaftStateTest
{

    @Test
    void shouldUpdateCacheState() throws Exception
    {
        //Test that updates applied to the raft state will be reflected in the entry cache.

        //given
        InFlightCache cache = new ConsecutiveInFlightCache();
        RaftState raftState = new RaftState( member( 0 ),
                                             new InMemoryStateStorage<>( new TermState() ), new FakeMembership(), new InMemoryRaftLog(),
                                             new InMemoryStateStorage<>( new VoteState() ), cache, NullLogProvider.getInstance(), false, false,
                                             Set::of );

        List<RaftLogCommand> logCommands = new LinkedList<>()
        {{
            add( new AppendLogEntry( 1, new RaftLogEntry( 0L, valueOf( 0 ) ) ) );
            add( new AppendLogEntry( 2, new RaftLogEntry( 0L, valueOf( 1 ) ) ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 2 ) ) ) );
            add( new AppendLogEntry( 4, new RaftLogEntry( 0L, valueOf( 4 ) ) ) );
            add( new TruncateLogCommand( 3 ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 5 ) ) ) );
        }};

        OutcomeBuilder raftTestMemberOutcome = builder().setRole( CANDIDATE ).renewElectionTimer( ACTIVE_ELECTION );

        for ( RaftLogCommand logCommand : logCommands )
        {
            raftTestMemberOutcome.addLogCommand( logCommand );
        }

        //when
        raftState.update( raftTestMemberOutcome.build() );

        //then
        Assertions.assertNotNull( cache.get( 1L ) );
        Assertions.assertNotNull( cache.get( 2L ) );
        Assertions.assertNotNull( cache.get( 3L ) );
        Assertions.assertEquals( valueOf( 5 ), cache.get( 3L ).content() );
        Assertions.assertNull( cache.get( 4L ) );
    }

    @Test
    void shouldRemoveFollowerStateAfterBecomingLeader() throws Exception
    {
        // given
        RaftState raftState = new RaftState( member( 0 ),
                                             new InMemoryStateStorage<>( new TermState() ),
                                             new FakeMembership(), new InMemoryRaftLog(),
                                             new InMemoryStateStorage<>( new VoteState() ),
                                             new ConsecutiveInFlightCache(), NullLogProvider.getInstance(),
                                             false, false, Set::of );

        raftState.update( builder().setRole( CANDIDATE ).replaceFollowerStates( initialFollowerStates() ).renewElectionTimer( ACTIVE_ELECTION ).build() );

        // when
        raftState.update( builder().setRole( CANDIDATE ).replaceFollowerStates( new FollowerStates<>() ).renewElectionTimer( ACTIVE_ELECTION ).build() );

        // then
        Assertions.assertEquals( 0, raftState.followerStates().size() );
    }

    private Collection<RaftMessages.Directed> emptyOutgoingMessages()
    {
        return new ArrayList<>();
    }

    private FollowerStates<MemberId> initialFollowerStates()
    {
        return new FollowerStates<>( new FollowerStates<>(), member( 1 ), new FollowerState() );
    }

    private Collection<RaftLogCommand> emptyLogCommands()
    {
        return Collections.emptyList();
    }

    private static class FakeMembership implements RaftMembership
    {
        @Override
        public Set<MemberId> votingMembers()
        {
            return emptySet();
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return emptySet();
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
