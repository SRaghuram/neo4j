/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;

public class ComparableRaftState implements ReadableRaftState
{
    protected final RaftMemberId myself;
    private final Set<RaftMemberId> votingMembers;
    private final Set<RaftMemberId> replicationMembers;
    private final Log log;
    protected long term;
    protected RaftMemberId leader;
    private LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private long leaderCommit = -1;
    private RaftMemberId votedFor;
    private Set<RaftMemberId> votesForMe = new HashSet<>();
    private Set<RaftMemberId> preVotesForMe = new HashSet<>();
    private Set<RaftMemberId> heartbeatResponses = new HashSet<>();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<RaftMemberId> followerStates = new FollowerStates<>();
    protected final RaftLog entryLog;
    private final InFlightCache inFlightCache;
    private long commitIndex = -1;
    private boolean isPreElection;
    private boolean timersStarted;

    ComparableRaftState( RaftMemberId myself, Set<RaftMemberId> votingMembers, Set<RaftMemberId> replicationMembers, boolean timersStarted,
                         RaftLog entryLog, InFlightCache inFlightCache, LogProvider logProvider )
    {
        this.myself = myself;
        this.votingMembers = votingMembers;
        this.replicationMembers = replicationMembers;
        this.entryLog = entryLog;
        this.inFlightCache = inFlightCache;
        this.log = logProvider.getLog( getClass() );
        this.timersStarted = timersStarted;
    }

    ComparableRaftState( ReadableRaftState original ) throws IOException
    {
        this( original.myself(), original.votingMembers(), original.replicationMembers(), original.areTimersStarted(),
                new ComparableRaftLog( original.entryLog() ), new ConsecutiveInFlightCache(), NullLogProvider.getInstance() );
    }

    @Override
    public RaftMemberId myself()
    {
        return myself;
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
    public long term()
    {
        return term;
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
        return 0;
    }

    @Override
    public RaftMemberId votedFor()
    {
        return votedFor;
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
        return false;
    }

    public void update( Outcome outcome ) throws IOException
    {
        term = outcome.getTerm();
        votedFor = outcome.getVotedFor();
        leader = outcome.getLeader();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();
        isPreElection = outcome.isPreElection();

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog, log );
            logCommand.applyTo( inFlightCache, log );
        }

        commitIndex = outcome.getCommitIndex();
    }

    @Override
    public String toString()
    {
        return format( "state{myself=%s, term=%s, leader=%s, leaderCommit=%d, appended=%d, committed=%d, " +
                        "votedFor=%s, votesForMe=%s, lastLogIndexBeforeWeBecameLeader=%d, followerStates=%s}",
                myself, term, leader, leaderCommit,
                entryLog.appendIndex(), commitIndex, votedFor, votesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ComparableRaftState that = (ComparableRaftState) o;
        return Objects.equals( term, that.term ) &&
                Objects.equals( lastLogIndexBeforeWeBecameLeader, that.lastLogIndexBeforeWeBecameLeader ) &&
                Objects.equals( myself, that.myself ) &&
                Objects.equals( votingMembers, that.votingMembers ) &&
                Objects.equals( leader, that.leader ) &&
                Objects.equals( leaderCommit, that.leaderCommit ) &&
                Objects.equals( entryLog, that.entryLog ) &&
                Objects.equals( votedFor, that.votedFor ) &&
                Objects.equals( votesForMe, that.votesForMe ) &&
                Objects.equals( followerStates, that.followerStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( myself, votingMembers, term, leader, entryLog, votedFor, votesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates );
    }
}
