/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v1.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.DatabaseCatchupRequest;

public class PrepareStoreCopyRequest implements DatabaseCatchupRequest
{
    private final StoreId storeId;
    private final String databaseName;

    public PrepareStoreCopyRequest( StoreId expectedStoreId, String databaseName )
    {
        this.storeId = expectedStoreId;
        this.databaseName = databaseName;
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

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public String toString()
    {
        return "PrepareStoreCopyRequest{storeId=" + storeId + ", databaseName='" + databaseName + "'}";
    }
}
