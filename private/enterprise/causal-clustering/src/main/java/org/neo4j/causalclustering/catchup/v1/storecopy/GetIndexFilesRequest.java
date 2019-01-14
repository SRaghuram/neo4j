/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.StoreCopyRequest;

public class GetIndexFilesRequest implements StoreCopyRequest
{
    private final StoreId expectedStoreId;
    private final long indexId;
    private final long requiredTransactionId;
    private final String databaseName;

    public GetIndexFilesRequest( StoreId expectedStoreId, long indexId, long requiredTransactionId, String databaseName )
    {
        this.expectedStoreId = expectedStoreId;
        this.indexId = indexId;
        this.requiredTransactionId = requiredTransactionId;
        this.databaseName = databaseName;
    }

    @Override
    public StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    @Override
    public long requiredTransactionId()
    {
        return requiredTransactionId;
    }

    public long indexId()
    {
        return indexId;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.INDEX_SNAPSHOT;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public String toString()
    {
        return "GetIndexFilesRequest{" + "expectedStoreId=" + expectedStoreId + ", indexId=" + indexId + ", requiredTransactionId=" + requiredTransactionId +
                '}';
    }
}
