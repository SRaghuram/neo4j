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
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Holds the outcome of a RAFT role's handling of a message. The role handling logic is stateless
 * and responds to RAFT messages in the context of a supplied state. The outcome is consumed
 * to update the state and do operations embedded as commands within the outcome.
 *
 * A state update could be to change role, change term, etc.
 * A command could be to append to the RAFT log, tell the log shipper that there was a mismatch, etc.
 */
public class Outcome implements ConsensusOutcome
{
    /* Common */
    private final Role role;

    private final long term;
    private final RaftMemberId leader;

    private final long leaderCommit;

    private final Collection<RaftLogCommand> logCommands;
    private final Collection<RaftMessages.Directed> outgoingMessages;

    private final long commitIndex;

    /* Follower */
    private final RaftMemberId votedFor;
    private final ElectionTimerMode electionTimerMode;
    private final SnapshotRequirement snapshotRequirement;
    private final boolean isPreElection;
    private final Set<RaftMemberId> preVotesForMe;

    /* Candidate */
    private final Set<RaftMemberId> votesForMe;
    private final long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private final FollowerStates<RaftMemberId> followerStates;
    private final Collection<ShipCommand> shipCommands;
    private final boolean electedLeader;
    private final long steppingDownInTerm;
    private final Set<RaftMemberId> heartbeatResponses;
    private final RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection;
    private final RaftMemberId transferringLeadershipTo;
    private final RaftMessages.StatusResponse statusResponse;

    Outcome( Role role, long term, RaftMemberId leader, long leaderCommit, RaftMemberId votedFor,
             Set<RaftMemberId> votesForMe, Set<RaftMemberId> preVotesForMe, long lastLogIndexBeforeWeBecameLeader,
             FollowerStates<RaftMemberId> followerStates, ElectionTimerMode electionTimerMode,
             Collection<RaftLogCommand> logCommands, Collection<RaftMessages.Directed> outgoingMessages,
             Collection<ShipCommand> shipCommands, long commitIndex, Set<RaftMemberId> heartbeatResponses, boolean isPreElection, boolean electedLeader,
             long steppingDownInTerm, SnapshotRequirement snapshotRequirement, RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection,
             RaftMemberId transferringLeadershipTo, RaftMessages.StatusResponse statusResponse )
    {
        this.role = role;
        this.term = term;
        this.leader = leader;
        this.leaderCommit = leaderCommit;
        this.votedFor = votedFor;
        this.votesForMe = votesForMe;
        this.preVotesForMe = preVotesForMe;
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        this.followerStates = followerStates;
        this.electionTimerMode = electionTimerMode;
        this.heartbeatResponses = Set.copyOf( heartbeatResponses );
        this.logCommands = logCommands;
        this.outgoingMessages = outgoingMessages;
        this.shipCommands = shipCommands;
        this.commitIndex = commitIndex;
        this.isPreElection = isPreElection;
        this.steppingDownInTerm = steppingDownInTerm;
        this.electedLeader = electedLeader;
        this.snapshotRequirement = snapshotRequirement;
        this.leadershipTransferRejection = leadershipTransferRejection;
        this.transferringLeadershipTo = transferringLeadershipTo;
        this.statusResponse = statusResponse;
    }

    @Override
    public String toString()
    {
        return "Outcome{" +
               "role=" + role +
               ", term=" + term +
               ", leader=" + leader +
               ", leaderCommit=" + leaderCommit +
               ", logCommands=" + logCommands +
               ", outgoingMessages=" + outgoingMessages +
               ", commitIndex=" + commitIndex +
               ", votedFor=" + votedFor +
               ", electionTimerMode=" + electionTimerMode +
               ", snapshotRequirement=" + snapshotRequirement +
               ", votesForMe=" + votesForMe +
               ", preVotesForMe=" + preVotesForMe +
               ", lastLogIndexBeforeWeBecameLeader=" + lastLogIndexBeforeWeBecameLeader +
               ", followerStates=" + followerStates +
               ", shipCommands=" + shipCommands +
               ", electedLeader=" + electedLeader +
               ", steppingDownInTerm=" + steppingDownInTerm +
               ", leaderTransferRejection=" + leadershipTransferRejection +
               ", transferringLeadershipTo=" + (transferringLeadershipTo != null ? transferringLeadershipTo : "Nobody")  +
               ", statusResponse=" + statusResponse  +
               '}';
    }

    public Role getRole()
    {
        return role;
    }

    public long getTerm()
    {
        return term;
    }

    public RaftMemberId getLeader()
    {
        return leader;
    }

    public long getLeaderCommit()
    {
        return leaderCommit;
    }

    public Collection<RaftLogCommand> getLogCommands()
    {
        return logCommands;
    }

    public Collection<RaftMessages.Directed> getOutgoingMessages()
    {
        return outgoingMessages;
    }

    public RaftMemberId getVotedFor()
    {
        return votedFor;
    }

    public Optional<ElectionTimerMode> electionTimerChanged()
    {
        return Optional.ofNullable( electionTimerMode );
    }

    @Override
    public Optional<SnapshotRequirement> snapshotRequirement()
    {
        return Optional.ofNullable( snapshotRequirement );
    }

    public Set<RaftMemberId> getVotesForMe()
    {
        return votesForMe;
    }

    public long getLastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    public FollowerStates<RaftMemberId> getFollowerStates()
    {
        return followerStates;
    }

    public Collection<ShipCommand> getShipCommands()
    {
        return shipCommands;
    }

    public boolean isElectedLeader()
    {
        return electedLeader;
    }

    public boolean isSteppingDown()
    {
        return steppingDownInTerm != -1;
    }

    public OptionalLong stepDownTerm()
    {
        return isSteppingDown() ? OptionalLong.of( steppingDownInTerm ) : OptionalLong.empty();
    }

    @Override
    public long getCommitIndex()
    {
        return commitIndex;
    }

    public Set<RaftMemberId> getHeartbeatResponses()
    {
        return heartbeatResponses;
    }

    public boolean isPreElection()
    {
        return isPreElection;
    }

    public Set<RaftMemberId> getPreVotesForMe()
    {
        return preVotesForMe;
    }

    public Optional<RaftMessages.LeadershipTransfer.Rejection> getLeaderTransferRejection()
    {
        return Optional.ofNullable(leadershipTransferRejection);
    }

    public Optional<RaftMemberId> transferringLeadershipTo()
    {
        return Optional.ofNullable( transferringLeadershipTo );
    }

    public Optional<RaftMessages.StatusResponse> getStatusResponse()
    {
        return Optional.ofNullable( statusResponse );
    }

    @Override
    public boolean equals( Object object )
    {
        if ( this == object )
        {
            return true;
        }
        if ( object == null || getClass() != object.getClass() )
        {
            return false;
        }
        Outcome outcome = (Outcome) object;
        return term == outcome.term &&
               leaderCommit == outcome.leaderCommit &&
               commitIndex == outcome.commitIndex &&
               electionTimerMode == outcome.electionTimerMode &&
               isPreElection == outcome.isPreElection &&
               lastLogIndexBeforeWeBecameLeader == outcome.lastLogIndexBeforeWeBecameLeader &&
               electedLeader == outcome.electedLeader &&
               steppingDownInTerm == outcome.steppingDownInTerm &&
               role == outcome.role &&
               Objects.equals( leader, outcome.leader ) &&
               Objects.equals( logCommands, outcome.logCommands ) &&
               Objects.equals( outgoingMessages, outcome.outgoingMessages ) &&
               Objects.equals( votedFor, outcome.votedFor ) &&
               Objects.equals( snapshotRequirement, outcome.snapshotRequirement ) &&
               Objects.equals( preVotesForMe, outcome.preVotesForMe ) &&
               Objects.equals( votesForMe, outcome.votesForMe ) &&
               Objects.equals( followerStates, outcome.followerStates ) &&
               Objects.equals( shipCommands, outcome.shipCommands ) &&
               Objects.equals( leadershipTransferRejection, outcome.leadershipTransferRejection ) &&
               Objects.equals( heartbeatResponses, outcome.heartbeatResponses ) &&
               Objects.equals( transferringLeadershipTo, outcome.transferringLeadershipTo ) &&
               Objects.equals( statusResponse, outcome.statusResponse );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( role, term, leader, leaderCommit, logCommands, outgoingMessages, commitIndex, votedFor, electionTimerMode, snapshotRequirement,
                isPreElection, preVotesForMe, votesForMe, lastLogIndexBeforeWeBecameLeader, followerStates, shipCommands, electedLeader, steppingDownInTerm,
                heartbeatResponses, leadershipTransferRejection, transferringLeadershipTo, statusResponse );
    }
}
