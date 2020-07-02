/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.membership.RaftTestMembers;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import com.neo4j.causalclustering.messaging.Outbound;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;

import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesResponse;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AppendEntriesMessageFlowTest
{
    private MemberId myself = member( 0 );
    private MemberId otherMember = member( 1 );

    private ReplicatedInteger data = ReplicatedInteger.valueOf( 1 );

    private Outbound<MemberId,RaftMessages.RaftMessage> outbound = mock( Outbound.class );

    ReplicatedInteger data( int value )
    {
        return ReplicatedInteger.valueOf( value );
    }

    private RaftMachine raft;

    @BeforeEach
    void setup() throws IOException
    {
        // given
        RaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, new RaftTestMembers( 0 ) ) );

        raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE, Clocks.fakeClock() )
                .raftLog( raftLog )
                .outbound( outbound )
                .build();
    }

    @Test
    void shouldReturnFalseOnAppendRequestFromOlderTerm() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( -1 ).prevLogIndex( 0 )
                                           .prevLogTerm( 0 ).leaderCommit( 0 ).build() );

        // then
        verify( outbound ).send( same( otherMember ),
                                 eq( appendEntriesResponse().from( myself ).term( 0 ).appendIndex( 0 ).matchIndex( -1 ).failure()
                                                            .build() ) );
    }

    @Test
    void shouldReturnTrueOnAppendRequestWithFirstLogEntry() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 0 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 1, data ) ).leaderCommit( -1 ).build() );

        // then
        verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().
                                                                                         appendIndex( 1 ).matchIndex( 1 ).from( myself ).term( 1 ).success()
                                                                                 .build() ) );
    }

    @Test
    void shouldReturnFalseOnAppendRequestWhenPrevLogEntryNotMatched() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                                           .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 0, data ) ).build() );

        // then
        verify( outbound ).send( same( otherMember ),
                                 eq( appendEntriesResponse().matchIndex( -1 ).appendIndex( 0 ).from( myself ).term( 0 ).failure().build() ) );
    }

    @Test
    void shouldAcceptSequenceOfAppendEntries() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 1 ) ) ).leaderCommit( -1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 1 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 2 ) ) ).leaderCommit( -1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 2 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 3 ) ) ).leaderCommit( 0 ).build() );

        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 3 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 1, data( 4 ) ) ).leaderCommit( 1 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 4 )
                                           .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 1, data( 5 ) ) ).leaderCommit( 2 ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 1 ).prevLogIndex( 5 )
                                           .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 1, data( 6 ) ) ).leaderCommit( 4 ).build() );

        // then
        InOrder invocationOrder = inOrder( outbound );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 1 ).matchIndex( 1 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 2 ).matchIndex( 2 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).appendIndex( 3 ).matchIndex( 3 ).success().build() ) );

        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 4 ).matchIndex( 4 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 5 ).matchIndex( 5 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 1 ).appendIndex( 6 ).matchIndex( 6 ).success().build() ) );
    }

    @Test
    void shouldReturnFalseIfLogHistoryDoesNotMatch() throws Exception
    {
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 0 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 1 ) ) ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 1 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 2 ) ) ).build() );
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 0 ).prevLogIndex( 2 )
                                           .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data( 3 ) ) ).build() ); // will conflict

        // when
        raft.handle( appendEntriesRequest().from( otherMember ).leaderTerm( 2 ).prevLogIndex( 3 )
                                           .prevLogTerm( 1 ).logEntry( new RaftLogEntry( 2, data( 4 ) ) )
                                           .build() ); // should reply false because of prevLogTerm

        // then
        InOrder invocationOrder = inOrder( outbound );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 1 ).appendIndex( 1 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 2 ).appendIndex( 2 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 0 ).matchIndex( 3 ).appendIndex( 3 ).success().build() ) );
        invocationOrder.verify( outbound ).send( same( otherMember ), eq( appendEntriesResponse().from(
                myself ).term( 2 ).matchIndex( -1 ).appendIndex( 3 ).failure().build() ) );
    }
}
