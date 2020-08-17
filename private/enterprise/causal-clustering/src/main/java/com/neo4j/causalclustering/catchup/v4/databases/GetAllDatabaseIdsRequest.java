/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

public class GetAllDatabaseIdsRequest extends CatchupProtocolMessage
{
    public GetAllDatabaseIdsRequest()
    {
        super( RequestMessageType.ALL_DATABASE_IDS_REQUEST );
    }
}
