/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.vote;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.roles.Voting;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VotingTest
{
    MemberId candidate = new MemberId( UUID.randomUUID() );

    long logTerm = 10;
    long currentTerm = 20;
    long appendIndex = 1000;

    Log log = NullLog.getInstance();

    @Test
    public void shouldAcceptRequestWithIdenticalLog()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    public void shouldRejectRequestFromOldTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm - 1,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    public void shouldRejectRequestIfCandidateLogEndsAtLowerTerm()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm - 1,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    public void shouldRejectRequestIfLogsEndInSameTermButCandidateLogIsShorter()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex - 1,
                false,
                log
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInSameTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex + 1,
                false,
                log
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogsEndInHigherTermAndCandidateLogIsShorter()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex - 1,
                false,
                log
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsSameLength()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex,
                false,
                log
        ) );
    }

    @Test
    public void shouldAcceptRequestIfLogEndsAtHigherTermAndCandidateLogIsLonger()
    {
        assertTrue( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm + 1,
                appendIndex,
                appendIndex + 1,
                false,
                log
        ) );
    }

    @Test
    public void shouldRejectRequestIfAlreadyVotedForOtherCandidate()
    {
        assertFalse( Voting.shouldVoteFor(
                candidate,
                currentTerm,
                currentTerm,
                logTerm,
                logTerm,
                appendIndex,
                appendIndex,
                true,
                log
        ) );
    }
}
