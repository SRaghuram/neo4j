/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

public interface ReadableRaftState
{
    RaftMemberId myself();

    Set<RaftMemberId> votingMembers();

    Set<RaftMemberId> replicationMembers();

    long term();

    RaftMemberId leader();

    LeaderInfo leaderInfo();

    long leaderCommit();

    RaftMemberId votedFor();

    Set<RaftMemberId> votesForMe();

    Set<RaftMemberId> heartbeatResponses();

    long lastLogIndexBeforeWeBecameLeader();

    FollowerStates<RaftMemberId> followerStates();

    ReadableRaftLog entryLog();

    long commitIndex();

    boolean isPreElection();

    Set<RaftMemberId> preVotesForMe();

    boolean areTimersStarted();

    boolean areTransferringLeadership();
}
