/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.explorer.action;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import org.neo4j.causalclustering.identity.MemberId;

public class DropMessage implements Action
{
    private final MemberId member;

    public DropMessage( MemberId member )
    {
        this.member = member;
    }

    @Override
    public ClusterState advance( ClusterState previous )
    {
        ClusterState newClusterState = new ClusterState( previous );
        Queue<RaftMessages.RaftMessage> inboundQueue = new LinkedList<>( previous.queues.get( member ) );
        RaftMessages.RaftMessage message = inboundQueue.poll();
        if ( message == null )
        {
            return previous;
        }

        newClusterState.queues.put( member, inboundQueue );
        return newClusterState;
    }
}
