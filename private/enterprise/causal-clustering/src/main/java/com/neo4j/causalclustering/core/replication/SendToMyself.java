/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import org.neo4j.function.Suppliers.Lazy;

public class SendToMyself
{
    private final Lazy<RaftMemberId> myself;
    private final Outbound<RaftMemberId,RaftMessage> outbound;

    public SendToMyself( Lazy<RaftMemberId> myself, Outbound<RaftMemberId,RaftMessage> outbound )
    {
        this.myself = myself;
        this.outbound = outbound;
    }

    public void replicate( ReplicatedContent content )
    {
        outbound.send( myself.get(), new RaftMessages.NewEntry.Request( myself.get(), content ) );
    }
}
