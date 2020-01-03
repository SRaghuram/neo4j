/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

public class SendToMyself
{
    private final MemberId myself;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;

    public SendToMyself( MemberId myself, Outbound<MemberId,RaftMessages.RaftMessage> outbound )
    {
        this.myself = myself;
        this.outbound = outbound;
    }

    public void replicate( ReplicatedContent content )
    {
        outbound.send( myself, new RaftMessages.NewEntry.Request( myself, content ) );
    }
}
