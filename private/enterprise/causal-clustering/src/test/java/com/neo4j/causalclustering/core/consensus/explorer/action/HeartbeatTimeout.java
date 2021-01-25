/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer.action;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.LinkedList;
import java.util.Queue;

public class HeartbeatTimeout implements Action
{
    private final RaftMemberId member;

    public HeartbeatTimeout( RaftMemberId member )
    {
        this.member = member;
    }

    @Override
    public ClusterState advance( ClusterState previous )
    {
        ClusterState newClusterState = new ClusterState( previous );
        Queue<RaftMessages.RaftMessage> newQueue = new LinkedList<>( previous.queues.get( member ) );
        newQueue.offer( new RaftMessages.Timeout.Heartbeat( member ) );
        newClusterState.queues.put( member, newQueue );
        return newClusterState;
    }
}
