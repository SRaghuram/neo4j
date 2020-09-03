/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.leader_transfer.ExpiringSet;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.io.state.StateStorage;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.function.Suppliers.lazySingleton;

public class RaftState implements ReadableRaftState
{
    private final Lazy<RaftMemberId> myself;
    private final StateStorage<TermState> termStorage;
    private final StateStorage<VoteState> voteStorage;
    private final RaftMembership membership;
    private final Log log;
    private final RaftLog entryLog;
    private final InFlightCache inFlightCache;
    private final ExpiringSet<RaftMemberId> leadershipTransfers;

    private TermState termState;
    private VoteState voteState;

    private RaftMemberId leader;
    private LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private Set<RaftMemberId> votesForMe = new HashSet<>();
    private Set<RaftMemberId> preVotesForMe = new HashSet<>();
    private Set<RaftMemberId> heartbeatResponses = new HashSet<>();
    private FollowerStates<RaftMemberId> followerStates = new FollowerStates<>();
    private long leaderCommit = -1;
    private long commitIndex = -1;
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private boolean isPreElection;
    private boolean timersStarted;

    public RaftState( RaftMemberId myself, StateStorage<TermState> termStorage, RaftMembership membership, RaftLog entryLog,
            StateStorage<VoteState> voteStorage, InFlightCache inFlightCache, LogProvider logProvider, ExpiringSet<RaftMemberId> leadershipTransfers )
    {
        this( lazySingleton( () -> myself ), termStorage, membership, entryLog, voteStorage, inFlightCache, logProvider, leadershipTransfers );
    }

    public RaftState( Lazy<RaftMemberId> myself,
                      StateStorage<TermState> termStorage,
                      RaftMembership membership,
                      RaftLog entryLog,
                      StateStorage<VoteState> voteStorage,
                      InFlightCache inFlightCache, LogProvider logProvider,
                      ExpiringSet<RaftMemberId> leadershipTransfers )
    {
        this.myself = myself;
        this.termStorage = termStorage;
        this.voteStorage = voteStorage;
        this.membership = membership;
        this.entryLog = entryLog;
        this.inFlightCache = inFlightCache;
        this.log = logProvider.getLog( getClass() );

        // Initial state
        this.leadershipTransfers = leadershipTransfers;
    }

    @Override
    public RaftMemberId myself()
    {
        return myself.get();
    }

    @Override
    public Set<RaftMemberId> votingMembers()
    {
        return membership.votingMembers();
    }

    @Override
    public Set<RaftMemberId> replicationMembers()
    {
        return membership.replicationMembers();
    }

    @Override
    public long term()
    {
        return termState().currentTerm();
    }

    private TermState termState()
    {
        if ( termState == null )
        {
            termState = termStorage.getInitialState();
        }
        return termState;
    }

    @Override
    public RaftMemberId leader()
    {
        return leader;
    }

    @Override
    public LeaderInfo leaderInfo()
    {
        return leaderInfo;
    }

    @Override
    public long leaderCommit()
    {
        return leaderCommit;
    }

    @Override
    public RaftMemberId votedFor()
    {
        return voteState().votedFor();
    }

    private VoteState voteState()
    {
        if ( voteState == null )
        {
            voteState = voteStorage.getInitialState();
        }
        return voteState;
    }

    @Override
    public Set<RaftMemberId> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public Set<RaftMemberId> heartbeatResponses()
    {
        return heartbeatResponses;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<RaftMemberId> followerStates()
    {
        return followerStates;
    }

    @Override
    public ReadableRaftLog entryLog()
    {
        return entryLog;
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    @Override
    public boolean isPreElection()
    {
        return isPreElection;
    }

    @Override
    public Set<RaftMemberId> preVotesForMe()
    {
        return preVotesForMe;
    }

    @Override
    public boolean areTimersStarted()
    {
        return timersStarted;
    }

    @Override
    public boolean areTransferringLeadership()
    {
        return leadershipTransfers.nonEmpty();
    }

    public void update( Outcome outcome ) throws IOException
    {
        if ( termState().update( outcome.getTerm() ) )
        {
            termStorage.writeState( termState() );
        }
        if ( voteState().update( outcome.getVotedFor(), outcome.getTerm() ) )
        {
            voteStorage.writeState( voteState() );
        }

        logIfLeaderChanged( outcome.getLeader() );
        leader = outcome.getLeader();
        leaderInfo = new LeaderInfo( outcome.getLeader(), outcome.getTerm() );

        leaderCommit = outcome.getLeaderCommit();
        votesForMe = outcome.getVotesForMe();
        preVotesForMe = outcome.getPreVotesForMe();
        heartbeatResponses = outcome.getHeartbeatResponses();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();
        isPreElection = outcome.isPreElection();

        if ( outcome.getRole() != Role.LEADER )
        {
            leadershipTransfers.clear();
        }
        else
        {
            outcome.transferringLeadershipTo().ifPresent( leadershipTransfers::add );
            outcome.getLeaderTransferRejection().ifPresent( rejection -> leadershipTransfers.remove( rejection.from() ) );
        }

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog, log );
            logCommand.applyTo( inFlightCache, log );
        }
        commitIndex = outcome.getCommitIndex();
    }

    public void setTimersStarted()
    {
        this.timersStarted = true;
    }

    private void logIfLeaderChanged( RaftMemberId leader )
    {
        if ( this.leader == null )
        {
            if ( leader != null )
            {
                log.info( "First leader elected: %s", leader );
            }
            return;
        }

        if ( !this.leader.equals( leader ) )
        {
            log.info( "Leader changed from %s to %s", this.leader, leader );
        }
    }

    public ExposedRaftState copy()
    {
        return new ReadOnlyRaftState( leaderCommit(), commitIndex(), entryLog().appendIndex(),
                lastLogIndexBeforeWeBecameLeader(), term(), votingMembers() );
    }

    private static class ReadOnlyRaftState implements ExposedRaftState
    {

        final long leaderCommit;
        final long commitIndex;
        final long appendIndex;
        final long lastLogIndexBeforeWeBecameLeader;
        final long term;

        final Set<RaftMemberId> votingMembers; // returned set is never mutated

        private ReadOnlyRaftState( long leaderCommit, long commitIndex, long appendIndex,
                long lastLogIndexBeforeWeBecameLeader, long term, Set<RaftMemberId> votingMembers )
        {
            this.leaderCommit = leaderCommit;
            this.commitIndex = commitIndex;
            this.appendIndex = appendIndex;
            this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
            this.term = term;
            this.votingMembers = votingMembers;
        }

        @Override
        public long lastLogIndexBeforeWeBecameLeader()
        {
            return lastLogIndexBeforeWeBecameLeader;
        }

        @Override
        public long leaderCommit()
        {
            return this.leaderCommit;
        }

        @Override
        public long commitIndex()
        {
            return this.commitIndex;
        }

        @Override
        public long appendIndex()
        {
            return this.appendIndex;
        }

        @Override
        public long term()
        {
            return this.term;
        }

        @Override
        public Set<RaftMemberId> votingMembers()
        {
            return this.votingMembers;
        }
    }
}
