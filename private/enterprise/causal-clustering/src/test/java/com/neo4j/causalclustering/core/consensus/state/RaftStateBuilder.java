/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.Set;

import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

public class RaftStateBuilder
{
    private RaftStateBuilder()
    {
    }

    public static RaftStateBuilder builder()
    {
        return new RaftStateBuilder();
    }

    private Outcome outcome = OutcomeTestBuilder.builder().build();
    public MemberId myself;
    private Set<MemberId> votingMembers = emptySet();
    private Set<MemberId> replicationMembers = emptySet();
    private RaftLog entryLog = new InMemoryRaftLog();
    private boolean supportPreVoting;
    private boolean refusesToBeLeader;

    public RaftStateBuilder myself( MemberId myself )
    {
        this.myself = myself;
        return this;
    }

    public RaftStateBuilder addInitialOutcome( Outcome outcome )
    {
        this.outcome = outcome;
        return this;
    }

    public RaftStateBuilder supportsPreVoting( boolean supportPreVoting )
    {
        this.supportPreVoting = supportPreVoting;
        return this;
    }

    public RaftStateBuilder votingMembers( Set<MemberId> currentMembers )
    {
        this.votingMembers = currentMembers;
        return this;
    }

    private RaftStateBuilder replicationMembers( Set<MemberId> replicationMembers )
    {
        this.replicationMembers = replicationMembers;
        return this;
    }

    public RaftStateBuilder entryLog( RaftLog entryLog )
    {
        this.entryLog = entryLog;
        return this;
    }

    public RaftStateBuilder setRefusesToBeLeader( boolean refusesToBeLeader )
    {
        this.refusesToBeLeader = refusesToBeLeader;
        return this;
    }

    public RaftState build() throws IOException
    {
        StateStorage<TermState> termStore = new InMemoryStateStorage<>( new TermState() );
        StateStorage<VoteState> voteStore = new InMemoryStateStorage<>( new VoteState( ) );
        StubMembership membership = new StubMembership( votingMembers, replicationMembers );

        RaftState state = new RaftState( myself, termStore, membership, entryLog,
                                         voteStore, new ConsecutiveInFlightCache(), NullLogProvider.getInstance(), supportPreVoting, refusesToBeLeader,
                                         Set::of );

        state.update( outcome );

        return state;
    }

    public RaftStateBuilder votingMembers( MemberId... members )
    {
        return votingMembers( asSet( members ) );
    }

    public RaftStateBuilder replicationMembers( MemberId... members )
    {
        return replicationMembers( asSet( members ) );
    }

    public RaftStateBuilder messagesSentToFollower( MemberId member, long nextIndex )
    {
        return this;
    }

    private static class StubMembership implements RaftMembership
    {
        private Set<MemberId> votingMembers;
        private final Set<MemberId> replicationMembers;

        private StubMembership( Set<MemberId> votingMembers, Set<MemberId> replicationMembers )
        {
            this.votingMembers = votingMembers;
            this.replicationMembers = replicationMembers;
        }

        @Override
        public Set<MemberId> votingMembers()
        {
            return votingMembers;
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return replicationMembers;
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
