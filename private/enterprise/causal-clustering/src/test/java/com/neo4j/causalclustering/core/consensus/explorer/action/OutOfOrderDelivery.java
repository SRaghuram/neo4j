/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer.action;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.LinkedList;

public class OutOfOrderDelivery implements Action
{
    private final MemberId member;

    public OutOfOrderDelivery( MemberId member )
    {
        this.member = member;
    }

    @Override
    public ClusterState advance( ClusterState previous )
    {
        ClusterState newClusterState = new ClusterState( previous );
        LinkedList<RaftMessages.RaftMessage> inboundQueue = new LinkedList<>( previous.queues.get( member ) );
        if ( inboundQueue.size() < 2 )
        {
            return previous;
        }
        RaftMessages.RaftMessage message = inboundQueue.poll();
        inboundQueue.add( 1, message );

        newClusterState.queues.put( member, inboundQueue );
        return newClusterState;
    }
}
