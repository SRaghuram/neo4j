/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.StoreId;

public class StoreIdTestFactory
{
    private static final RecordFormats format = RecordFormatSelector.defaultFormat();

    private StoreIdTestFactory()
    {
    }

    private static long currentStoreVersionAsLong()
    {
        return MetaDataStore.versionStringToLong( format.storeVersion() );
    }

    public static StoreId newStoreIdForCurrentVersion()
    {
        return new StoreId( currentStoreVersionAsLong() );
    }

    public static StoreId newStoreIdForCurrentVersion( long creationTime, long randomId, long upgradeTime, long
            upgradeId )
    {
        return new StoreId( creationTime, randomId, MetaDataStore.versionStringToLong( format.storeVersion() ),
                upgradeTime, upgradeId );
    }
}
