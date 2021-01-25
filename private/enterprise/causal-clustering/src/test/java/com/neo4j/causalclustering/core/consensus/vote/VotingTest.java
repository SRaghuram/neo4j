/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.core.consensus.roles.Voting;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

class VotingTest
{
    private RaftMemberId candidate = IdFactory.randomRaftMemberId();

    private long logTerm = 10;
    private long currentTerm = 20;
    private long appendIndex = 1000;

    private Log log = NullLog.getInstance();

    @Test
    void shouldAcceptRequestWithIdenticalLog()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log, false ) );
    }

    @Test
    void shouldRejectRequestFromOldTerm()
    {
        Assertions.assertFalse( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm - 1,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log, false ) );
    }

    @Test
    void shouldRejectRequestIfCandidateLogEndsAtLowerTerm()
    {
        Assertions.assertFalse( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm - 1,
                appendIndex,
                appendIndex,
                false,
                log, false ) );
    }

    @Test
    void shouldRejectRequestIfLogsEndInSameTermButCandidateLogIsShorter()
    {
        Assertions.assertFalse( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex - 1,
                false,
                log, false ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsSameLength()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log, false ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsLonger()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex + 1,
                false,
                log, false ) );
    }

    @Test
    void shouldAcceptRequestIfLogsEndInHigherTermAndCandidateLogIsShorter()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex - 1,
                false,
                log, false ) );
    }

    @Test
    void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsSameLength()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex,
                false,
                log, false ) );
    }

    @Test
    void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsLonger()
    {
        Assertions.assertTrue( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex + 1,
                false,
                log, false ) );
    }

    @Test
    void shouldRejectRequestIfAlreadyVotedForOtherCandidate()
    {
        Assertions.assertFalse( Voting.shouldAnyVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                true,
                log, false ) );
    }
}
