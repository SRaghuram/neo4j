/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;

public class PrepareStoreCopyRequest implements CatchUpRequest
{
    private final StoreId storeId;

    public PrepareStoreCopyRequest( StoreId expectedStoreId )
    {
        this.storeId = expectedStoreId;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.PREPARE_STORE_COPY;
    }
}
