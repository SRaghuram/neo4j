/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import org.neo4j.kernel.database.DatabaseId;

public class GetStoreIdRequest extends CatchupProtocolMessage.WithDatabaseId
{
    public GetStoreIdRequest( DatabaseId databaseId )
    {
        super( RequestMessageType.STORE_ID, databaseId );
    }
}
