/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.vote;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

class VoteStateTest implements BaseMarshalTest<VoteState>
{

    @Override
    public Collection<VoteState> originals()
    {
        return Stream.generate( this::randomVoteState )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    private VoteState randomVoteState()
    {
        var voteState = new VoteState();
        voteState.update( IdFactory.randomRaftMemberId(), ThreadLocalRandom.current().nextLong( Long.MAX_VALUE ) );
        return voteState;
    }

    @Override
    public ChannelMarshal<VoteState> marshal()
    {
        return VoteState.Marshal.INSTANCE;
    }

    @Test
    void shouldStoreVote()
    {
        // given
        VoteState voteState = new VoteState();
        RaftMemberId member = IdFactory.randomRaftMemberId();

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
        RaftMemberId member1 = IdFactory.randomRaftMemberId();
        RaftMemberId member2 = IdFactory.randomRaftMemberId();

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
        RaftMemberId member = IdFactory.randomRaftMemberId();

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
        RaftMemberId member1 = IdFactory.randomRaftMemberId();
        RaftMemberId member2 = IdFactory.randomRaftMemberId();

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
        RaftMemberId member = IdFactory.randomRaftMemberId();

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
        RaftMemberId member1 = IdFactory.randomRaftMemberId();
        RaftMemberId member2 = IdFactory.randomRaftMemberId();

        // when
        Assertions.assertTrue( voteState.update( null, 0 ) );
        Assertions.assertFalse( voteState.update( null, 0 ) );
        Assertions.assertTrue( voteState.update( member1, 0 ) );
        Assertions.assertFalse( voteState.update( member1, 0 ) );
        Assertions.assertTrue( voteState.update( member2, 1 ) );
        Assertions.assertFalse( voteState.update( member2, 1 ) );
    }
}
