/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.leader_transfer.ExpiringSet;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeTestBuilder;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.state.StateStorage;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.FakeClock;

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
    public RaftMemberId myself;
    private Set<RaftMemberId> votingMembers = emptySet();
    private Set<RaftMemberId> replicationMembers = emptySet();
    private RaftLog entryLog = new InMemoryRaftLog();
    private boolean beforeTimersStarted;
    private ExpiringSet<RaftMemberId> leadershipTransfers = new ExpiringSet<>( Duration.ofSeconds( 1 ), new FakeClock() );

    public RaftStateBuilder myself( RaftMemberId myself )
    {
        this.myself = myself;
        return this;
    }

    public RaftStateBuilder addInitialOutcome( Outcome outcome )
    {
        this.outcome = outcome;
        return this;
    }

    public RaftStateBuilder votingMembers( Set<RaftMemberId> currentMembers )
    {
        this.votingMembers = currentMembers;
        return this;
    }

    private RaftStateBuilder additionalReplicationMembers( Set<RaftMemberId> replicationMembers )
    {
        this.replicationMembers = replicationMembers;
        return this;
    }

    public RaftStateBuilder entryLog( RaftLog entryLog )
    {
        this.entryLog = entryLog;
        return this;
    }

    public RaftStateBuilder setBeforeTimersStarted( boolean beforeTimersStarted )
    {
        this.beforeTimersStarted = beforeTimersStarted;
        return this;
    }

    public RaftState build() throws IOException
    {
        StateStorage<TermState> termStore = new InMemoryStateStorage<>( new TermState() );
        StateStorage<VoteState> voteStore = new InMemoryStateStorage<>( new VoteState() );
        StubMembership membership = new StubMembership( votingMembers, replicationMembers );

        RaftState state = new RaftState( myself, termStore, membership, entryLog,
                voteStore, new ConsecutiveInFlightCache(), NullLogProvider.getInstance(), leadershipTransfers );

        state.update( outcome );
        if ( !beforeTimersStarted )
        {
            state.setTimersStarted();
        }

        return state;
    }

    public RaftStateBuilder votingMembers( RaftMemberId... members )
    {
        return votingMembers( asSet( members ) );
    }

    public RaftStateBuilder additionalReplicationMembers( RaftMemberId... members )
    {
        return additionalReplicationMembers( asSet( members ) );
    }

    public RaftStateBuilder messagesSentToFollower( RaftMemberId member, long nextIndex )
    {
        return this;
    }

    private static class StubMembership implements RaftMembership
    {
        private Set<RaftMemberId> votingMembers;
        private final Set<RaftMemberId> replicationMembers;

        private StubMembership( Set<RaftMemberId> votingMembers, Set<RaftMemberId> additionalReplicationMembers )
        {
            this.votingMembers = votingMembers;
            this.replicationMembers = new HashSet<>( votingMembers );
            this.replicationMembers.addAll( additionalReplicationMembers );
        }

        @Override
        public Set<RaftMemberId> votingMembers()
        {
            return votingMembers;
        }

        @Override
        public Set<RaftMemberId> replicationMembers()
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
