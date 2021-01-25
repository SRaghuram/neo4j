/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithState;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Most behaviour for handling vote requests is identical for all roles.
 */
class VoteRequestTest
{
    private RaftMemberId myself = raftMember( 0 );
    private RaftMemberId member1 = raftMember( 1 );
    private RaftMemberId member2 = raftMember( 2 );

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldVoteForCandidateInLaterTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertTrue( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
    }

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldDenyForCandidateInPreviousTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() - 1;

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
        assertEquals( role, outcome.getRole() );
    }

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldVoteForOnlyOneCandidatePerTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome1 = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        state.update( outcome1 );

        Outcome outcome2 = role.handler.handle( voteRequest().from( member2 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome2, member2 )).voteGranted() );
    }

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldStayInCurrentRoleOnRequestFromCurrentTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term();

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertEquals( role, outcome.getRole() );
    }

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldMoveToFollowerIfRequestIsFromLaterTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertEquals( Role.FOLLOWER, outcome.getRole() );
    }

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldUpdateTermIfRequestIsFromLaterTerm( Role role ) throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertEquals( candidateTerm, outcome.getTerm() );
    }

    RaftState newState() throws IOException
    {
        return builder().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

}
