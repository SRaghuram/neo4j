/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;

public class StoreValidation
{
    /**
     * Check if it is safe to use and apply transactions from the remote store on the local store .
     * @param localStoreId The {@link StoreId} of the local store.
     * @param remoteStoreId The {@link StoreId} of the remote store.
     * @param storageEngineFactory The {@link StorageEngineFactory} of the local store. Used to compare the {@link StoreId#getStoreVersion()}.
     * @return {@code true} if it is (id-wise) safe to use transactions from the remote store on the local store, {@code false} otherwise.
     */
    public static boolean validRemoteToUseTransactionsFrom( StoreId localStoreId, StoreId remoteStoreId, StorageEngineFactory storageEngineFactory )
    {
        //Here we handle two situations
        //1. Normal operation where we are on the same version on the same store.
        //2. During rolling upgrade where we allow fetching transaction from both older and newer store versions of the same store.
        //   This works because in rolling upgrade, the new tx log version format is only selected when all members are on the new store version
        //   We allow both older and newer store since this code can be executed on both a member that just upgraded (thus may fetch from older members)
        //   Or it is executed on a member that is not yet upgraded (thus may fetch from newer members)
        return localStoreId.compatibleIncludingMinorUpgrade( storageEngineFactory, remoteStoreId ) ||
                remoteStoreId.compatibleIncludingMinorUpgrade( storageEngineFactory, localStoreId );
    }

    /**
     * Check if it is safe to use the remote store as the local store. I.e it is recoverable and migratable if needed.
     * @param desiredStoreVersion The desired (configured) {@link StoreVersion} of store.
     * @param remoteStoreVersion The actual version of the remote store
     * @return {@code true} if it is (version-wise) safe to use the remote store as the local store, {@code false} otherwise.
     */
    public static boolean validRemoteToUseStoreFrom( StoreVersion desiredStoreVersion, StoreVersion remoteStoreVersion )
    {
        //Here we handle two situations
        //1. Normal operation where we are on the same version on the same store.
        //2. During rolling upgrade where we also allow to fetch from an older compatible store, where we can recover and migrate it to match.
        //   It is not allowed to fetch a newer store, to (most likely) old jars.
        return remoteStoreVersion.isCompatibleWithIncludingMinorMigration( desiredStoreVersion );
    }

}
