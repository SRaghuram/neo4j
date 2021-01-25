/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.remote;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.StoreId;

public class RemoteInfo
{
    private final SocketAddress remoteAddress;
    private final StoreId remoteStoreId;
    private final NamedDatabaseId namedDatabaseId;

    RemoteInfo( SocketAddress remoteAddress, StoreId remoteStoreId, NamedDatabaseId namedDatabaseId )
    {
        this.remoteAddress = remoteAddress;
        this.remoteStoreId = remoteStoreId;
        this.namedDatabaseId = namedDatabaseId;
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
    }

    public SocketAddress address()
    {
        return remoteAddress;
    }

    public StoreId storeId()
    {
        return remoteStoreId;
    }

    @Override
    public String toString()
    {
        return "RemoteInfo{" +
               "remoteAddress=" + remoteAddress +
               ", remoteStoreId=" + remoteStoreId +
               ", namedDatabaseId=" + namedDatabaseId +
               '}';
    }
}
