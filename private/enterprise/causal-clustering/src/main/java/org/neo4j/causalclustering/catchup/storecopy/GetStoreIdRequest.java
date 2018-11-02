/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.messaging.CatchUpRequest;

public class GetStoreIdRequest implements CatchUpRequest
{
    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.STORE_ID;
    }
}
