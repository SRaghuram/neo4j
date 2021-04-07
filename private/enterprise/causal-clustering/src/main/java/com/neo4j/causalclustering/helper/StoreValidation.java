/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

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
     * @param storeVersionCheck The {@link StoreVersionCheck} used to fetch the configured version (if configured)
     * @param remoteStoreVersion The actual version of the remote store
     * @param config The current configuration
     * @param storageEngineFactory A {@link StorageEngineFactory}. Used to compare the {@link StoreId#getStoreVersion()}.
     * @return {@code true} if it is (version-wise) safe to use the remote store as the local store, {@code false} otherwise.
     */
    public static boolean validRemoteToUseStoreFrom( StoreVersionCheck storeVersionCheck, String remoteStoreVersion, Config config,
            StorageEngineFactory storageEngineFactory )
    {
        //Here we handle two situations
        //1. Normal operation where we are on the same version on the same store.
        //2. During rolling upgrade where we also allow to fetch from an older compatible store, where we can recover and migrate it to match.
        //   It is not allowed to fetch a newer store, to (most likely) old jars.
        StoreVersion version;
        if ( config.get( GraphDatabaseSettings.allow_upgrade ) && storeVersionCheck.isVersionConfigured() )
        {
            //We will upgrade and have a configured format. Check if they are compatible after migration
            version = storageEngineFactory.versionInformation( storeVersionCheck.configuredVersion() );
        }
        else
        {
            //Either no migration will happen, or we're migrating to the latest version of the current format.
            //Check that is it compatible to apply transactions on (recovery etc..)
            try
            {
                version = storageEngineFactory.versionInformation( remoteStoreVersion ).latest();
            }
            catch ( Exception e )
            {
                return false; //Unknown remote version, not compatible!
            }
        }
        return storageEngineFactory.rollingUpgradeCompatibility().isVersionCompatibleForRollingUpgrade( remoteStoreVersion, version.storeVersion() );
    }

}
