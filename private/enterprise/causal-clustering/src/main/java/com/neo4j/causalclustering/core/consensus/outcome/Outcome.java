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
import com.neo4j.causalclustering.identity.MemberId;

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
    private final MemberId leader;

    private final long leaderCommit;

    private final Collection<RaftLogCommand> logCommands;
    private final Collection<RaftMessages.Directed> outgoingMessages;

    private final long commitIndex;

    /* Follower */
    private final MemberId votedFor;
    private final ElectionTimerMode electionTimerMode;
    private final SnapshotRequirement snapshotRequirement;
    private final boolean isPreElection;
    private final Set<MemberId> preVotesForMe;

    /* Candidate */
    private final Set<MemberId> votesForMe;
    private final long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private final FollowerStates<MemberId> followerStates;
    private final Collection<ShipCommand> shipCommands;
    private final boolean electedLeader;
    private final long steppingDownInTerm;
    private final Set<MemberId> heartbeatResponses;

    Outcome( Role role, long term, MemberId leader, long leaderCommit, MemberId votedFor,
            Set<MemberId> votesForMe, Set<MemberId> preVotesForMe, long lastLogIndexBeforeWeBecameLeader,
            FollowerStates<MemberId> followerStates, ElectionTimerMode electionTimerMode,
            Collection<RaftLogCommand> logCommands, Collection<RaftMessages.Directed> outgoingMessages,
            Collection<ShipCommand> shipCommands, long commitIndex, Set<MemberId> heartbeatResponses, boolean isPreElection, boolean electedLeader,
            long steppingDownInTerm, SnapshotRequirement snapshotRequirement )
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

    public MemberId getLeader()
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

    public MemberId getVotedFor()
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

    public Set<MemberId> getVotesForMe()
    {
        return votesForMe;
    }

    public long getLastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    public FollowerStates<MemberId> getFollowerStates()
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

    public Set<MemberId> getHeartbeatResponses()
    {
        return heartbeatResponses;
    }

    public boolean isPreElection()
    {
        return isPreElection;
    }

    public Set<MemberId> getPreVotesForMe()
    {
        return preVotesForMe;
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
               Objects.equals( heartbeatResponses, outcome.heartbeatResponses );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( role, term, leader, leaderCommit, logCommands, outgoingMessages, commitIndex, votedFor, electionTimerMode, snapshotRequirement,
                isPreElection, preVotesForMe, votesForMe, lastLogIndexBeforeWeBecameLeader, followerStates, shipCommands, electedLeader, steppingDownInTerm,
                heartbeatResponses );
    }
}
