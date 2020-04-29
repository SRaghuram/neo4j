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
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftState implements ReadableRaftState
{
    private final MemberId myself;
    private final StateStorage<TermState> termStorage;
    private final StateStorage<VoteState> voteStorage;
    private final RaftMembership membership;
    private final Log log;
    private final RaftLog entryLog;
    private final InFlightCache inFlightCache;
    private final boolean supportPreVoting;
    private final ExpiringSet<MemberId> leadershipTransfers;

    private TermState termState;
    private VoteState voteState;

    private MemberId leader;
    private LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private Set<MemberId> votesForMe = new HashSet<>();
    private Set<MemberId> preVotesForMe = new HashSet<>();
    private Set<MemberId> heartbeatResponses = new HashSet<>();
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    private long leaderCommit = -1;
    private long commitIndex = -1;
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private boolean isPreElection;
    private final boolean refuseToBeLeader;
    private Supplier<Set<String>> serverGroupsSupplier;

    public RaftState( MemberId myself,
                      StateStorage<TermState> termStorage,
                      RaftMembership membership,
                      RaftLog entryLog,
                      StateStorage<VoteState> voteStorage,
                      InFlightCache inFlightCache, LogProvider logProvider, boolean supportPreVoting,
                      boolean refuseToBeLeader,
                      Supplier<Set<String>> serverGroupsSupplier,
                      ExpiringSet<MemberId> leadershipTransfers )
    {
        this.myself = myself;
        this.termStorage = termStorage;
        this.voteStorage = voteStorage;
        this.membership = membership;
        this.entryLog = entryLog;
        this.inFlightCache = inFlightCache;
        this.supportPreVoting = supportPreVoting;
        this.log = logProvider.getLog( getClass() );

        // Initial state
        this.isPreElection = supportPreVoting;
        this.refuseToBeLeader = refuseToBeLeader;
        this.serverGroupsSupplier = serverGroupsSupplier;
        this.leadershipTransfers = leadershipTransfers;
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }

    @Override
    public Set<MemberId> votingMembers()
    {
        return membership.votingMembers();
    }

    @Override
    public Set<MemberId> replicationMembers()
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
    public MemberId leader()
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
    public MemberId votedFor()
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
    public Set<MemberId> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public Set<MemberId> heartbeatResponses()
    {
        return heartbeatResponses;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<MemberId> followerStates()
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
    public boolean supportPreVoting()
    {
        return supportPreVoting;
    }

    @Override
    public boolean isPreElection()
    {
        return isPreElection;
    }

    @Override
    public Set<MemberId> preVotesForMe()
    {
        return preVotesForMe;
    }

    @Override
    public boolean refusesToBeLeader()
    {
        return refuseToBeLeader;
    }

    @Override
    public Set<String> serverGroups()
    {
        return serverGroupsSupplier.get();
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

        var transfer = outcome.transferringLeadershipTo();
        var transferRejection = outcome.getLeaderTransferRejection();

        if ( transfer.isPresent() )
        {
            leadershipTransfers.add( transfer.get() );
        }
        else if ( transferRejection != null )
        {
            leadershipTransfers.remove( transferRejection.from() );
        }
        else if ( outcome.getRole() != Role.LEADER )
        {
            leadershipTransfers.clear();
        }

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog, log );
            logCommand.applyTo( inFlightCache, log );
        }
        commitIndex = outcome.getCommitIndex();
    }

    private void logIfLeaderChanged( MemberId leader )
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

        final Set<MemberId> votingMembers; // returned set is never mutated

        private ReadOnlyRaftState( long leaderCommit, long commitIndex, long appendIndex,
                long lastLogIndexBeforeWeBecameLeader, long term, Set<MemberId> votingMembers )
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
        public Set<MemberId> votingMembers()
        {
            return this.votingMembers;
        }
    }
}
