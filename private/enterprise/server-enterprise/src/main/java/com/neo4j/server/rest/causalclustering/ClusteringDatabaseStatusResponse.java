/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

@JsonSerialize
public class ClusteringDatabaseStatusResponse
{
    private final boolean isCore;
    private final long lastAppliedRaftIndex;
    private final boolean isParticipatingInRaftGroup;
    private final Collection<String> votingMembers;
    private final boolean isHealthy;
    private final String memberId;
    private final String leader;
    private final Long millisSinceLastLeaderMessage;
    private final Double raftCommandsPerSecond;
    private final boolean isDiscoveryHealthy;

    ClusteringDatabaseStatusResponse( long lastAppliedRaftIndex, boolean isParticipatingInRaftGroup, Collection<RaftMemberId> votingMembers, boolean isHealthy,
            UUID memberId, UUID leader, Duration millisSinceLastLeaderMessage, Double raftCommandsPerSecond, boolean isCore, boolean isDiscoveryHealthy )
    {
        this.lastAppliedRaftIndex = lastAppliedRaftIndex;
        this.isParticipatingInRaftGroup = isParticipatingInRaftGroup;
        this.votingMembers = votingMembers.stream().map( member -> member.uuid().toString() ).sorted().collect( Collectors.toList() );
        this.isHealthy = isHealthy;
        this.memberId = memberId.toString();
        this.leader = leader == null ? null : leader.toString();
        this.millisSinceLastLeaderMessage = millisSinceLastLeaderMessage == null ? null : millisSinceLastLeaderMessage.toMillis();
        this.raftCommandsPerSecond = raftCommandsPerSecond;
        this.isCore = isCore;
        this.isDiscoveryHealthy = isDiscoveryHealthy;
    }

    /**
     * Transactions are associated with raft log indexes. By tracking this value across a cluster you will be able to evaluate with whether
     * the cluster is caught up and functioning as expected.
     *
     * @return the latest transaction id available on this node
     */
    public long getLastAppliedRaftIndex()
    {
        return lastAppliedRaftIndex;
    }

    /**
     * A node is considered participating if it believes it is caught up and knows who the leader is. Leader timeouts will prevent this value from being true
     * even if the core is caught up. This is always false for replicas, since they never participate in raft. The refuse to be leader flag does not affect this
     * logic (i.e. if a core proposes itself to be leader, it still doesn't know who the leader is since it the leader has not been voted in)
     *
     * @return true if the core is in a "good state" (up to date and part of raft). For cores this is likely the flag you will want to look at
     */
    public boolean isParticipatingInRaftGroup()
    {
        return isParticipatingInRaftGroup;
    }

    /**
     * For cores, this will list all known live core members. Read replicas also include all known read replicas.
     * Users will want to monitor this field (size or values) when performing rolling upgrades for read replicas.
     *
     * @return a list of discovery addresses ("hostname:port") that are part of this node's membership set
     */
    public Collection<String> getVotingMembers()
    {
        return votingMembers;
    }

    public boolean isHealthy()
    {
        return isHealthy;
    }

    public boolean isDiscoveryHealthy()
    {
        return isDiscoveryHealthy;
    }

    public String getMemberId()
    {
        return memberId;
    }

    public String getLeader()
    {
        return leader;
    }

    public Long getMillisSinceLastLeaderMessage()
    {
        return millisSinceLastLeaderMessage;
    }

    public Double getRaftCommandsPerSecond()
    {
        return raftCommandsPerSecond;
    }

    public boolean isCore()
    {
        return isCore;
    }
}
