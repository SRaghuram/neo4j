/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VoteStateTest
{
    @Test
    void shouldStoreVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = IdFactory.randomMemberId();

        // when
        voteState.update( member, 0 );

        // then
        Assertions.assertEquals( member, voteState.votedFor() );
    }

    @Test
    void shouldStartWithNoVote()
    {
        // given
        VoteState voteState = new VoteState();

        // then
        Assertions.assertNull( voteState.votedFor() );
    }

    @Test
    void shouldUpdateVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = IdFactory.randomMemberId();
        MemberId member2 = IdFactory.randomMemberId();

        // when
        voteState.update( member1, 0 );
        voteState.update( member2, 1 );

        // then
        Assertions.assertEquals( member2, voteState.votedFor() );
    }

    @Test
    void shouldClearVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = IdFactory.randomMemberId();

        voteState.update( member, 0 );

        // when
        voteState.update( null, 1 );

        // then
        Assertions.assertNull( voteState.votedFor() );
    }

    @Test
    void shouldNotUpdateVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = IdFactory.randomMemberId();
        MemberId member2 = IdFactory.randomMemberId();

        voteState.update( member1, 0 );

        try
        {
            // when
            voteState.update( member2, 0 );
            Assertions.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    void shouldNotClearVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = IdFactory.randomMemberId();

        voteState.update( member, 0 );

        try
        {
            // when
            voteState.update( null, 0 );
            Assertions.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    void shouldReportNoUpdateWhenVoteStateUnchanged()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = IdFactory.randomMemberId();
        MemberId member2 = IdFactory.randomMemberId();

        // when
        Assertions.assertTrue( voteState.update( null, 0 ) );
        Assertions.assertFalse( voteState.update( null, 0 ) );
        Assertions.assertTrue( voteState.update( member1, 0 ) );
        Assertions.assertFalse( voteState.update( member1, 0 ) );
        Assertions.assertTrue( voteState.update( member2, 1 ) );
        Assertions.assertFalse( voteState.update( member2, 1 ) );
    }
}
