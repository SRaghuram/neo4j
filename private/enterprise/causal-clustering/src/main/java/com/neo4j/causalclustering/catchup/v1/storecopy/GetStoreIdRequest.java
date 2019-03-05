/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v1.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

public class GetStoreIdRequest extends CatchupProtocolMessage
{
    public GetStoreIdRequest( String databaseName )
    {
        super( RequestMessageType.STORE_ID, databaseName );
    }
}
