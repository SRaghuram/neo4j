/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.ElectionTimerMode;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.util.VisibleForTesting;

import static java.util.Collections.emptySet;

/**
 * Builds the outcome of a RAFT role's handling of a message. The immutable {@link Outcome} is then built to update the state and do operations embedded as
 * commands within the outcome.
 */
public class OutcomeBuilder
{
    /* Common */
    private Role role;

    private long term;
    private MemberId leader;

    private long leaderCommit;

    private Collection<RaftLogCommand> logCommands = new ArrayList<>();
    private Collection<RaftMessages.Directed> outgoingMessages = new ArrayList<>();

    private long commitIndex;

    /* Follower */
    private MemberId votedFor;
    private ElectionTimerMode electionTimerMode;
    private SnapshotRequirement snapshotRequirement;
    private boolean isPreElection;
    private Set<MemberId> preVotesForMe;

    /* Candidate */
    private Set<MemberId> votesForMe;
    private long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private FollowerStates<MemberId> followerStates;
    private Collection<ShipCommand> shipCommands = new ArrayList<>();
    private boolean electedLeader;
    private long steppingDownInTerm;
    private Set<MemberId> heartbeatResponses;
    private RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection;
    private MemberId transferingTo;

    @VisibleForTesting
    OutcomeBuilder( Role currentRole, long term, MemberId leader, long leaderCommit, MemberId votedFor, ElectionTimerMode electionTimerMode,
            boolean isPreElection, long steppingDownInTerm, Set<MemberId> preVotesForMe, Set<MemberId> votesForMe, Set<MemberId> heartbeatResponses,
            long lastLogIndexBeforeWeBecameLeader, FollowerStates<MemberId> followerStates, long commitIndex )
    {
        this.role = currentRole;
        this.term = term;
        this.leader = leader;
        this.leaderCommit = leaderCommit;
        this.votedFor = votedFor;
        this.electionTimerMode = electionTimerMode;
        this.isPreElection = isPreElection;
        this.steppingDownInTerm = steppingDownInTerm;
        this.preVotesForMe = preVotesForMe;
        this.votesForMe = votesForMe;
        this.heartbeatResponses = heartbeatResponses;
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        this.followerStates = followerStates;
        this.commitIndex = commitIndex;
    }

    public static OutcomeBuilder builder( Role currentRole, ReadableRaftState ctx )
    {
        var isPreElection = (currentRole == Role.FOLLOWER) && ctx.isPreElection();
        Set<MemberId> preVotesForMe = isPreElection ? new HashSet<>( ctx.preVotesForMe() ) : emptySet();
        Set<MemberId> votesForMe = (currentRole == Role.CANDIDATE) ? new HashSet<>( ctx.votesForMe() ) : emptySet();
        Set<MemberId> heartbeatResponses = (currentRole == Role.LEADER) ? new HashSet<>( ctx.heartbeatResponses() ) : emptySet();
        var lastLogIndexBeforeWeBecameLeader = (currentRole == Role.LEADER) ? ctx.lastLogIndexBeforeWeBecameLeader() : -1;
        FollowerStates<MemberId> followerStates = (currentRole == Role.LEADER) ? ctx.followerStates() : new FollowerStates<>();
        return new OutcomeBuilder( currentRole, ctx.term(), ctx.leader(), ctx.leaderCommit(), ctx.votedFor(), null, isPreElection, -1,
                preVotesForMe, votesForMe, heartbeatResponses, lastLogIndexBeforeWeBecameLeader, followerStates, ctx.commitIndex() );
    }

    public OutcomeBuilder setRole( Role role )
    {
        this.role = role;
        return this;
    }

    public OutcomeBuilder setTerm( long nextTerm )
    {
        this.term = nextTerm;
        return this;
    }

    public OutcomeBuilder setLeader( MemberId leader )
    {
        this.leader = leader;
        return this;
    }

    public OutcomeBuilder setLeaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public OutcomeBuilder addLogCommand( RaftLogCommand logCommand )
    {
        this.logCommands.add( logCommand );
        return this;
    }

    public OutcomeBuilder addOutgoingMessage( RaftMessages.Directed message )
    {
        this.outgoingMessages.add( message );
        return this;
    }

    public OutcomeBuilder setVotedFor( MemberId votedFor )
    {
        this.votedFor = votedFor;
        return this;
    }

    public OutcomeBuilder renewElectionTimer( ElectionTimerMode electionTimerMode )
    {
        this.electionTimerMode = electionTimerMode;
        return this;
    }

    public OutcomeBuilder markNeedForFreshSnapshot( long leaderPrevIndex, long localAppendIndex )
    {
        this.snapshotRequirement = new SnapshotRequirement( leaderPrevIndex, localAppendIndex );
        return this;
    }

    public OutcomeBuilder setLastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        return this;
    }

    public OutcomeBuilder replaceFollowerStates( FollowerStates<MemberId> followerStates )
    {
        this.followerStates = followerStates;
        return this;
    }

    public OutcomeBuilder addShipCommand( ShipCommand shipCommand )
    {
        shipCommands.add( shipCommand );
        return this;
    }

    public OutcomeBuilder electedLeader()
    {
        assert steppingDownInTerm == -1;
        this.electedLeader = true;
        return this;
    }

    public OutcomeBuilder steppingDown( long stepDownTerm )
    {
        assert !electedLeader;
        steppingDownInTerm = stepDownTerm;
        return this;
    }

    @Override
    public String toString()
    {
        return "Outcome{" +
               "nextRole=" + role +
               ", term=" + term +
               ", leader=" + leader +
               ", leaderCommit=" + leaderCommit +
               ", logCommands=" + logCommands +
               ", outgoingMessages=" + outgoingMessages +
               ", commitIndex=" + commitIndex +
               ", votedFor=" + votedFor +
               ", renewElectionTimeout=" + electionTimerMode +
               ", snapshotRequirement=" + snapshotRequirement +
               ", votesForMe=" + votesForMe +
               ", preVotesForMe=" + preVotesForMe +
               ", lastLogIndexBeforeWeBecameLeader=" + lastLogIndexBeforeWeBecameLeader +
               ", followerStates=" + followerStates +
               ", shipCommands=" + shipCommands +
               ", electedLeader=" + electedLeader +
               ", steppingDownInTerm=" + steppingDownInTerm +
               '}';
    }

    public OutcomeBuilder setCommitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public OutcomeBuilder addHeartbeatResponse( MemberId from )
    {
        this.heartbeatResponses.add( from );
        return this;
    }

    public OutcomeBuilder clearHeartbeatResponses()
    {
        heartbeatResponses.clear();
        return this;
    }

    public OutcomeBuilder setPreElection( boolean isPreElection )
    {
        this.isPreElection = isPreElection;
        return this;
    }

    public OutcomeBuilder setPreVotesForMe( Set<MemberId> preVotes )
    {
        preVotesForMe = preVotes;
        return this;
    }

    public OutcomeBuilder setVotesForMe( Set<MemberId> votes )
    {
        votesForMe = votes;
        return this;
    }

    public OutcomeBuilder addLeaderTransferRejection( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
    {
        this.leadershipTransferRejection = leadershipTransferRejection;
        return this;
    }

    public OutcomeBuilder startTransferringLeadership( MemberId proposed )
    {
        this.transferingTo = proposed;
        return this;
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
        OutcomeBuilder outcome = (OutcomeBuilder) o;
        return term == outcome.term && leaderCommit == outcome.leaderCommit && commitIndex == outcome.commitIndex &&
               electionTimerMode == outcome.electionTimerMode && Objects.equals( snapshotRequirement, outcome.snapshotRequirement ) &&
               isPreElection == outcome.isPreElection && lastLogIndexBeforeWeBecameLeader == outcome.lastLogIndexBeforeWeBecameLeader &&
               electedLeader == outcome.electedLeader && role == outcome.role &&
               Objects.equals( steppingDownInTerm, outcome.steppingDownInTerm ) && Objects.equals( leader, outcome.leader ) &&
               Objects.equals( logCommands, outcome.logCommands ) && Objects.equals( outgoingMessages, outcome.outgoingMessages ) &&
               Objects.equals( votedFor, outcome.votedFor ) && Objects.equals( preVotesForMe, outcome.preVotesForMe ) &&
               Objects.equals( votesForMe, outcome.votesForMe ) && Objects.equals( followerStates, outcome.followerStates ) &&
               Objects.equals( shipCommands, outcome.shipCommands ) && Objects.equals( heartbeatResponses, outcome.heartbeatResponses ) &&
               Objects.equals( transferingTo, outcome.transferingTo ) && Objects.equals( leadershipTransferRejection, outcome.leadershipTransferRejection );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( role, term, leader, leaderCommit, logCommands, outgoingMessages, commitIndex, votedFor, electionTimerMode,
                snapshotRequirement, isPreElection, preVotesForMe, votesForMe, lastLogIndexBeforeWeBecameLeader, followerStates, shipCommands, electedLeader,
                steppingDownInTerm, heartbeatResponses, transferingTo, leadershipTransferRejection );
    }

    public Outcome build()
    {

        return new Outcome( role, term, leader, leaderCommit, votedFor, votesForMe, preVotesForMe, lastLogIndexBeforeWeBecameLeader, followerStates,
                electionTimerMode, logCommands, outgoingMessages, shipCommands, commitIndex, heartbeatResponses, isPreElection, electedLeader,
                steppingDownInTerm, snapshotRequirement, leadershipTransferRejection, transferingTo );
    }
}
