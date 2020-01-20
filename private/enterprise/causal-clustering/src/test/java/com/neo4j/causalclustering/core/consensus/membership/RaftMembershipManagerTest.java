/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.replication.SendToMyself;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState.Marshal;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.logging.NullLogProvider.getInstance;

@ExtendWith( LifeExtension.class )
class RaftMembershipManagerTest
{
    private final Log log = NullLog.getInstance();
    private final MemberId myself = member( 0 );
    private final SendToMyself sendToMyself = mock( SendToMyself.class );

    @Inject
    private LifeSupport life;

    @Test
    void membershipManagerShouldUseLatestAppendedMembershipSetEntries()
            throws Exception
    {
        // given
        final InMemoryRaftLog log = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = life.add( raftMembershipManager( log ) );
        // when
        membershipManager.processLog( 0, asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 5 ) ) )
        ) );

        // then
        assertEquals( new RaftTestMembers( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
    }

    @Test
    void membershipManagerShouldRevertToOldMembershipSetAfterTruncationCausesLossOfAllAppendedMembershipSets()
            throws Exception
    {
        // given
        final InMemoryRaftLog raftLog = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = life.add( raftMembershipManager( raftLog ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 5 ) ) ),
                new TruncateLogCommand( 1 )
        );

        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
        }
        membershipManager.processLog( 0, logCommands );

        // then
        assertEquals( new RaftTestMembers( 1, 2, 3, 4 ).getMembers(), membershipManager.votingMembers() );
        assertFalse( membershipManager.uncommittedMemberChangeInLog() );
    }

    @Test
    void membershipManagerShouldRevertToEarlierAppendedMembershipSetAfterTruncationCausesLossOfLastAppended()
            throws Exception
    {
        // given
        final InMemoryRaftLog raftLog = new InMemoryRaftLog();

        RaftMembershipManager membershipManager = life.add( raftMembershipManager( raftLog ) );

        // when
        List<RaftLogCommand> logCommands = asList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 4 ) ) ),
                new AppendLogEntry( 1, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 5 ) ) ),
                new AppendLogEntry( 2, new RaftLogEntry( 0, new RaftTestMembers( 1, 2, 3, 6 ) ) ),
                new TruncateLogCommand( 2 )
        );
        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
        }
        membershipManager.processLog( 0, logCommands );

        // then
        assertEquals( new RaftTestMembers( 1, 2, 3, 5 ).getMembers(), membershipManager.votingMembers() );
    }

    @Test
    void shouldNotRemoveSelfFromConsensusGroup() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftMembershipManager membershipManager = life.add( raftMembershipManager( raftLog ) );

        RaftTestMembers membersA = new RaftTestMembers( 0, 1, 2, 3, 4 );
        RaftTestMembers membersB = new RaftTestMembers( 1, 2, 3, 4 ); // without myself

        List<RaftLogCommand> logCommands = singletonList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, membersA ) ) );

        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
        }
        membershipManager.processLog( 0, logCommands );
        membershipManager.onRole( LEADER );

        // when
        membershipManager.setTargetMembershipSet( membersB.getMembers() );

        // then
        verify( sendToMyself, never() ).replicate( any() );
    }

    @Test
    void shouldRemoveOtherFromConsensusGroup() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftMembershipManager membershipManager = life.add( raftMembershipManager( raftLog ) );

        RaftTestMembers membersA = new RaftTestMembers( 0, 1, 2, 3, 4 );
        RaftTestMembers membersB = new RaftTestMembers( 0, 2, 3, 4 ); // without number 1

        List<RaftLogCommand> logCommands = singletonList(
                new AppendLogEntry( 0, new RaftLogEntry( 0, membersA ) ) );

        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.applyTo( raftLog, log );
        }
        membershipManager.processLog( 0, logCommands );
        membershipManager.onRole( LEADER );

        // when
        membershipManager.setTargetMembershipSet( membersB.getMembers() );

        // then
        verify( sendToMyself ).replicate( membersB );
    }

    @Test
    void shouldWarnIfTargetANdVotingMembersDiverge() throws Exception
    {
        // given
        var warningMessage = "It is likely an operator removed too many core members too quickly";
        var raftLog = new InMemoryRaftLog();
        var logProvider = new AssertableLogProvider();
        var membershipManager = life.add( raftMembershipManager( raftLog, logProvider ) );

        var membersA = new RaftTestMembers( 0, 1, 2 );
        var membersB = new RaftTestMembers( 1, 2, 3 );
        var membersC = new RaftTestMembers( 3, 4, 5 ); // third set does not contain majority of second

        var logCommandA = new AppendLogEntry( 0, new RaftLogEntry( 0, membersA ) );
        var logCommandB = new AppendLogEntry( 1, new RaftLogEntry( 0, membersB ) );

        logCommandA.applyTo( raftLog, logProvider.getLog( getClass() ) );

        membershipManager.processLog( 0, List.of( logCommandA ) );
        membershipManager.onRole( LEADER );

        // when
        membershipManager.setTargetMembershipSet( membersB.getMembers() );

        // then
        logProvider.assertNone( inLog( RaftMembershipManager.class ).warn( containsString( warningMessage ) ) );

        // when
        membershipManager.processLog( 1, List.of( logCommandB ) );
        membershipManager.onRole( LEADER );
        membershipManager.setTargetMembershipSet( membersC.getMembers() );

        // then
        logProvider.assertAtLeastOnce( inLog( RaftMembershipManager.class )
                .warn( containsString( warningMessage ), membersC.getMembers(), membersB.getMembers() ) );

    }

    private RaftMembershipManager raftMembershipManager( InMemoryRaftLog log )
    {
        return raftMembershipManager( log, getInstance() );
    }

    private RaftMembershipManager raftMembershipManager( InMemoryRaftLog log, LogProvider logProvider )
    {
        RaftMembershipManager raftMembershipManager = new RaftMembershipManager(
                sendToMyself, myself, INSTANCE, log,
                logProvider, 3, 1000, Clocks.fakeClock(),
                1000, new InMemoryStateStorage<>( new Marshal().startState() ) );

        raftMembershipManager.setRecoverFromIndexSupplier( () -> 0 );
        return raftMembershipManager;
    }
}
