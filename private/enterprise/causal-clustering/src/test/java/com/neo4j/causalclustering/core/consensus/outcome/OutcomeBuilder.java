/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.emptySet;

public class OutcomeBuilder
{
    private Role nextRole = Role.FOLLOWER;

    private long term;
    private MemberId leader = new MemberId( UUID.randomUUID() );

    private long leaderCommit;

    private Collection<RaftLogCommand> logCommands = new ArrayList<>();
    private Collection<RaftMessages.Directed> outgoingMessages = new ArrayList<>();

    private long commitIndex;

    /* Follower */
    private MemberId votedFor;
    private boolean renewElectionTimeout;
    private boolean isPreElection;
    private Set<MemberId> preVotesForMe = emptySet();

    /* Candidate */
    private Set<MemberId> votesForMe = emptySet();
    private long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    private Collection<ShipCommand> shipCommands = new ArrayList<>();
    private boolean electedLeader;
    private OptionalLong steppingDownInTerm = OptionalLong.empty();
    private Set<MemberId> heartbeatResponses = emptySet();

    private OutcomeBuilder()
    {

    }

    public static OutcomeBuilder builder()
    {
        return new OutcomeBuilder();
    }

    public OutcomeBuilder setNextRole( Role nextRole )
    {
        this.nextRole = nextRole;
        return this;
    }

    public OutcomeBuilder setTerm( long term )
    {
        this.term = term;
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

    public OutcomeBuilder setLogCommands( Collection<RaftLogCommand> logCommands )
    {
        this.logCommands = logCommands;
        return this;
    }

    public OutcomeBuilder setOutgoingMessages( Collection<RaftMessages.Directed> outgoingMessages )
    {
        this.outgoingMessages = outgoingMessages;
        return this;
    }

    public OutcomeBuilder setCommitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public OutcomeBuilder setVotedFor( MemberId votedFor )
    {
        this.votedFor = votedFor;
        return this;
    }

    public OutcomeBuilder setRenewElectionTimeout( boolean renewElectionTimeout )
    {
        this.renewElectionTimeout = renewElectionTimeout;
        return this;
    }

    public OutcomeBuilder setPreElection( boolean preElection )
    {
        isPreElection = preElection;
        return this;
    }

    public OutcomeBuilder setPreVotesForMe( Set<MemberId> preVotesForMe )
    {
        this.preVotesForMe = preVotesForMe;
        return this;
    }

    public OutcomeBuilder setVotesForMe( Set<MemberId> votesForMe )
    {
        this.votesForMe = votesForMe;
        return this;
    }

    public OutcomeBuilder setLastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        return this;
    }

    public OutcomeBuilder setFollowerStates( FollowerStates<MemberId> followerStates )
    {
        this.followerStates = followerStates;
        return this;
    }

    public OutcomeBuilder setShipCommands( Collection<ShipCommand> shipCommands )
    {
        this.shipCommands = shipCommands;
        return this;
    }

    public OutcomeBuilder setElectedLeader( boolean electedLeader )
    {
        this.electedLeader = electedLeader;
        return this;
    }

    public OutcomeBuilder setSteppingDownInTerm( OptionalLong steppingDownInTerm )
    {
        this.steppingDownInTerm = steppingDownInTerm;
        return this;
    }

    public OutcomeBuilder setHeartbeatResponses( Set<MemberId> heartbeatResponses )
    {
        this.heartbeatResponses = heartbeatResponses;
        return this;
    }

    public Outcome build()
    {
        var outcome =  new Outcome( nextRole, term, leader, leaderCommit, votedFor, votesForMe, preVotesForMe, lastLogIndexBeforeWeBecameLeader, followerStates,
                renewElectionTimeout, logCommands, outgoingMessages, shipCommands, commitIndex, heartbeatResponses, isPreElection );

        if ( electedLeader )
        {
            outcome.electedLeader();
        }

        if ( steppingDownInTerm.isPresent() )
        {
            outcome.steppingDown( steppingDownInTerm.getAsLong() );
        }

        return outcome;
    }
}
