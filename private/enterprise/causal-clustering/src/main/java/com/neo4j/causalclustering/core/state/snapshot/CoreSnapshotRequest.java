/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

public class CoreSnapshotRequest implements CatchupProtocolMessage
{
    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.CORE_SNAPSHOT;
    }
}
