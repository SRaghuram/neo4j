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

class NonFollowerVoteRequestTest
{
    private RaftMemberId myself = raftMember( 0 );
    private RaftMemberId member1 = raftMember( 1 );

    @ParameterizedTest
    @EnumSource( value = Role.class, mode = EnumSource.Mode.EXCLUDE, names = {"FOLLOWER"} )
    void shouldRejectVoteRequestFromCurrentTerm( Role role ) throws Exception
    {
        RaftState state = newState();

        // when
        final long candidateTerm = state.term();

        Outcome outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), contextWithState( state ), log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
        assertEquals( role, outcome.getRole() );
    }

    @ParameterizedTest
    @EnumSource( value = Role.class, mode = EnumSource.Mode.EXCLUDE, names = {"FOLLOWER"} )
    void shouldRejectVoteRequestFromPreviousTerm( Role role ) throws Exception
    {
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

    RaftState newState() throws IOException
    {
        return builder().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
