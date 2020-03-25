/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

public interface ReadableRaftState
{
    MemberId myself();

    Set<MemberId> votingMembers();

    Set<MemberId> replicationMembers();

    long term();

    MemberId leader();

    LeaderInfo leaderInfo();

    long leaderCommit();

    MemberId votedFor();

    Set<MemberId> votesForMe();

    Set<MemberId> heartbeatResponses();

    long lastLogIndexBeforeWeBecameLeader();

    FollowerStates<MemberId> followerStates();

    ReadableRaftLog entryLog();

    long commitIndex();

    boolean supportPreVoting();

    boolean isPreElection();

    Set<MemberId> preVotesForMe();

    boolean refusesToBeLeader();

    Set<String> serverGroups();

    boolean areTransferringLeadership();
}
