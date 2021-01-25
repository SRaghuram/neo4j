/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.heartbeat;
import static com.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestTest.ContentGenerator.content;
import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithState;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.assertj.core.api.Assertions.assertThat;

class HeartbeatTest
{
    static Stream<Object[]> data()
    {
        return Stream.of( new Object[][]{
                {Role.FOLLOWER, 0}, {Role.FOLLOWER, 1}, {Role.LEADER, 1}, {Role.CANDIDATE, 1}
        } );
    }

    private RaftMemberId myself = raftMember( 0 );
    private RaftMemberId leader = raftMember( 1 );

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldNotResultInCommitIfReferringToFutureEntries( Role role, int leaderTermDifference ) throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex() + 1 ) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, contextWithState( state ), log() );

        assertThat( outcome.getLogCommands() ).isEmpty();
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldNotResultInCommitIfHistoryMismatches( Role role, int leaderTermDifference ) throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex() ) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, contextWithState( state ), log() );

        assertThat( outcome.getCommitIndex() ).isEqualTo( 0L );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldResultInCommitIfHistoryMatches( Role role, int leaderTermDifference ) throws Exception
    {
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        long leaderTerm = state.term() + leaderTermDifference;
        raftLog.append( new RaftLogEntry( leaderTerm - 1, content() ) );

        RaftMessages.Heartbeat heartbeat = heartbeat()
                .from( leader )
                .commitIndex( raftLog.appendIndex()) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( leaderTerm ) // And is in the same term
                .leaderTerm( leaderTerm )
                .build();

        Outcome outcome = role.handler.handle( heartbeat, contextWithState( state ), log() );

        assertThat( outcome.getLogCommands() ).isEmpty();

    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

}
