/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteRequest;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

/**
 * Most behaviour for handling vote requests is identical for all roles.
 */
@RunWith( Parameterized.class )
public class PreVoteRequestTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Collection data()
    {
        return asList( Role.values() );
    }

    @Parameterized.Parameter
    public Role role;

    private MemberId myself = member( 0 );
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    @Test
    public void shouldDenyForCandidateInLaterTermWhenPreVoteNotActive() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( preVoteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertFalse( ((RaftMessages.PreVote.Response) messageFor( outcome, member1 )).voteGranted() );
    }

    @Test
    public void shouldDenyForCandidateInPreviousTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() - 1;

        Outcome outcome = role.handler.handle( preVoteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertFalse( ((RaftMessages.PreVote.Response) messageFor( outcome, member1 )).voteGranted() );
        assertEquals( role, outcome.getRole() );
    }

    @Test
    public void shouldStayInCurrentRoleOnRequestFromCurrentTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term();

        Outcome outcome = role.handler.handle( preVoteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( role, outcome.getRole() );
    }

    @Test
    public void shouldMoveToFollowerIfRequestIsFromLaterTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( preVoteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( Role.FOLLOWER, outcome.getRole() );
    }

    @Test
    public void shouldUpdateTermIfRequestIsFromLaterTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome outcome = role.handler.handle( preVoteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( candidateTerm, outcome.getTerm() );
    }

    public RaftState newState() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

}
