/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.explorer.ClusterSafetyViolations.inconsistentCommittedLogEntries;
import static com.neo4j.causalclustering.core.consensus.explorer.ClusterSafetyViolations.multipleLeadersInSameTerm;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class ClusterSafetyViolationsTest
{
    @Test
    void shouldRecogniseInconsistentCommittedContent() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 3 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    void shouldRecogniseInconsistentTerm() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 2, valueOf( 2 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    void shouldRecogniseSomeMembersBeingInconsistent() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );
        clusterState.states.get( member( 2 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 1 ) ) );

        clusterState.states.get( member( 0 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 1 ) ).entryLog.append( new RaftLogEntry( 1, valueOf( 2 ) ) );
        clusterState.states.get( member( 2 ) ).entryLog.append( new RaftLogEntry( 2, valueOf( 2 ) ) );

        commit( clusterState, member( 0 ), 0 );
        commit( clusterState, member( 1 ), 0 );
        commit( clusterState, member( 2 ), 0 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 0 ), 1 );
        commit( clusterState, member( 1 ), 1 );

        // then
        assertFalse( inconsistentCommittedLogEntries( clusterState ) );

        // when
        commit( clusterState, member( 2 ), 1 );

        // then
        assertTrue( inconsistentCommittedLogEntries( clusterState ) );
    }

    @Test
    void shouldRecogniseTwoLeadersInTheSameTerm() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        // when
        clusterState.states.get( member( 0 ) ).term = 21;
        clusterState.states.get( member( 1 ) ).term = 21;
        clusterState.states.get( member( 2 ) ).term = 21;

        clusterState.roles.put( member( 0 ), Role.LEADER );
        clusterState.roles.put( member( 1 ), Role.LEADER );
        clusterState.roles.put( member( 2 ), Role.FOLLOWER );

        // then
        assertTrue( multipleLeadersInSameTerm( clusterState ) );
    }

    @Test
    void shouldRecogniseTwoLeadersInDifferentTerms() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ), member( 1 ), member(2) ) );

        // when
        clusterState.states.get( member( 0 ) ).term = 21;
        clusterState.states.get( member( 1 ) ).term = 22;
        clusterState.states.get( member( 2 ) ).term = 21;

        clusterState.roles.put( member( 0 ), Role.LEADER );
        clusterState.roles.put( member( 1 ), Role.LEADER );
        clusterState.roles.put( member( 2 ), Role.FOLLOWER );

        // then
        assertFalse( multipleLeadersInSameTerm( clusterState ) );
    }

    private void commit( ClusterState clusterState, MemberId member, long commitIndex ) throws IOException
    {
        ComparableRaftState state = clusterState.states.get( member );
        OutcomeBuilder outcome = OutcomeBuilder.builder( clusterState.roles.get( member ), state );
        outcome.setCommitIndex( commitIndex );
        state.update( outcome.build() );
    }
}
