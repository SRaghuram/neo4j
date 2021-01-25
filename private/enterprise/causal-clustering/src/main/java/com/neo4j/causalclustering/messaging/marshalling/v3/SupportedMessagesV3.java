/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v3;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.SupportedMessages;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;

public class SupportedMessagesV3 extends SupportedMessagesV2
{
    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
    {
        return true;
    }
}
