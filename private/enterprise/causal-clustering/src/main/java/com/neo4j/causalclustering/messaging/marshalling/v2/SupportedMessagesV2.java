/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.SupportedMessages;

public class SupportedMessagesV2 extends SupportedMessages
{
    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
    {
        return false;
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
    {
        return false;
    }
}
