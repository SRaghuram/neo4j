/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.membership.RaftTestMembers;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.mockito.Mockito.mock;

class RaftMachineLogTest
{
    private RaftMachineBuilder.CommitListener commitListener = mock( RaftMachineBuilder.CommitListener.class );

    private MemberId myself = member( 0 );
    private ReplicatedContent content = valueOf( 1 );
    private RaftLog testEntryLog;

    private RaftMachine raft;

    @BeforeEach
    void before() throws Exception
    {
        // given
        testEntryLog = new InMemoryRaftLog();
        testEntryLog.append( new RaftLogEntry( 0, new RaftTestMembers( myself ) ) );

        raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( testEntryLog )
                .commitListener( commitListener )
                .build();
    }

    @Test
    void shouldPersistAtSpecifiedLogIndex() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        Assertions.assertEquals( 1, testEntryLog.appendIndex() );
        Assertions.assertEquals( content, readLogEntry( testEntryLog, 1 ).content() );
    }

    @Test
    void shouldOnlyPersistSameLogEntryOnce() throws Exception
    {
        // when
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );
        raft.handle( appendEntriesRequest().leaderTerm( 0 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 0, content ) ).build() );

        // then
        Assertions.assertEquals( 1, testEntryLog.appendIndex() );
        Assertions.assertEquals( content, readLogEntry( testEntryLog, 1 ).content() );
    }

    @Test
    void shouldRemoveLaterEntryFromLogConflictingWithNewEntry() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 4 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 7 ) ) ); /* conflicting entry */

        // when
        ReplicatedInteger newData = valueOf( 11 );
        raft.handle( appendEntriesRequest().leaderTerm( 2 ).prevLogIndex( 2 ).prevLogTerm( 1 )
                .logEntry( new RaftLogEntry( 2, newData ) ).build() );

        // then
        Assertions.assertEquals( 3, testEntryLog.appendIndex() );
        Assertions.assertEquals( newData, readLogEntry( testEntryLog, 3 ).content() );
    }

    @Test
    void shouldNotTouchTheLogIfWeDoMatchEverywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) ); // 0
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) ); // 1
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) ); // 5
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) ); // 10

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Matches everything in the given range
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 5 ).prevLogTerm( 2 )
                .logEntry( new RaftLogEntry( 2, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .logEntry( new RaftLogEntry( 3, newData ) )
                .build() );

        // then
        Assertions.assertEquals( 11, testEntryLog.appendIndex() );
        Assertions.assertEquals( 3, testEntryLog.readEntryTerm( 11 ) );
    }

    /* Figure 3.6 */
    @Test
    void shouldNotTouchTheLogIfWeDoNotMatchAnywhere() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        // Will not match as the entry at index 5 has term  2
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 6 ).prevLogTerm( 5 )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        Assertions.assertEquals( 11, testEntryLog.appendIndex() );
        Assertions.assertEquals( 3, testEntryLog.readEntryTerm( 11 ) );
    }

    @Test
    void shouldTruncateOnFirstMismatchAndThenAppendOtherEntries() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );

        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 0 ).prevLogTerm( 0 )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 4, newData ) ) // term mismatch - existing term is 2
                .logEntry( new RaftLogEntry( 4, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        Assertions.assertEquals( 10, testEntryLog.appendIndex() );
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 1 ) );
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 2 ) );
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 3 ) );
        Assertions.assertEquals( 4, testEntryLog.readEntryTerm( 4 ) );
        Assertions.assertEquals( 4, testEntryLog.readEntryTerm( 5 ) );
        Assertions.assertEquals( 5, testEntryLog.readEntryTerm( 6 ) );
        Assertions.assertEquals( 5, testEntryLog.readEntryTerm( 7 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 8 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 9 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 10 ) );
    }

    @Test
    void shouldNotTruncateLogIfHistoryDoesNotMatch() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 4 ).prevLogTerm( 4 )
                .logEntry( new RaftLogEntry( 4, newData ) ) /* conflict */
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        Assertions.assertEquals( 11, testEntryLog.appendIndex() );
    }

    @Test
    void shouldTruncateLogIfFirstEntryMatchesAndSecondEntryMismatchesOnTerm() throws Exception
    {
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 1, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 2, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );
        testEntryLog.append( new RaftLogEntry( 3, valueOf( 99 ) ) );

        // when instance A as leader
        ReplicatedInteger newData = valueOf( 99 );
        raft.handle( appendEntriesRequest().leaderTerm( 8 ).prevLogIndex( 2 ).prevLogTerm( 1 )
                .logEntry( new RaftLogEntry( 1, newData ) )
                .logEntry( new RaftLogEntry( 4, newData ) ) /* conflict */
                .logEntry( new RaftLogEntry( 4, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 5, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .logEntry( new RaftLogEntry( 6, newData ) )
                .build() );

        // then
        Assertions.assertEquals( 10, testEntryLog.appendIndex() );

        // stay the same
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 1 ) );
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 2 ) );
        Assertions.assertEquals( 1, testEntryLog.readEntryTerm( 3 ) );

        // replaced
        Assertions.assertEquals( 4, testEntryLog.readEntryTerm( 4 ) );
        Assertions.assertEquals( 4, testEntryLog.readEntryTerm( 5 ) );
        Assertions.assertEquals( 5, testEntryLog.readEntryTerm( 6 ) );
        Assertions.assertEquals( 5, testEntryLog.readEntryTerm( 7 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 8 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 9 ) );
        Assertions.assertEquals( 6, testEntryLog.readEntryTerm( 10 ) );
    }
}
