/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VoteStateTest
{
    @Test
    public void shouldStoreVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        // when
        voteState.update( member, 0 );

        // then
        assertEquals( member, voteState.votedFor() );
    }

    @Test
    public void shouldStartWithNoVote()
    {
        // given
        VoteState voteState = new VoteState();

        // then
        assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldUpdateVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        // when
        voteState.update( member1, 0 );
        voteState.update( member2, 1 );

        // then
        assertEquals( member2, voteState.votedFor() );
    }

    @Test
    public void shouldClearVote()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        voteState.update( member, 0 );

        // when
        voteState.update( null, 1 );

        // then
        assertNull( voteState.votedFor() );
    }

    @Test
    public void shouldNotUpdateVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        voteState.update( member1, 0 );

        try
        {
            // when
            voteState.update( member2, 0 );
            Assert.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldNotClearVoteForSameTerm()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member = new MemberId( UUID.randomUUID() );

        voteState.update( member, 0 );

        try
        {
            // when
            voteState.update( null, 0 );
            Assert.fail( "Should have thrown IllegalArgumentException" );
        }
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldReportNoUpdateWhenVoteStateUnchanged()
    {
        // given
        VoteState voteState = new VoteState();
        MemberId member1 = new MemberId( UUID.randomUUID() );
        MemberId member2 = new MemberId( UUID.randomUUID() );

        // when
        assertTrue( voteState.update( null, 0 ) );
        assertFalse( voteState.update( null, 0 ) );
        assertTrue( voteState.update( member1, 0 ) );
        assertFalse( voteState.update( member1, 0 ) );
        assertTrue( voteState.update( member2, 1 ) );
        assertFalse( voteState.update( member2, 1 ) );
    }
}
