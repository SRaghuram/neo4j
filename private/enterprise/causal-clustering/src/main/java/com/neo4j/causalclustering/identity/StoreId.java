/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.Objects;

import static java.lang.String.format;

public final class StoreId
{
    public static final StoreId DEFAULT = new StoreId(
            org.neo4j.storageengine.api.StoreId.DEFAULT.getCreationTime(),
            org.neo4j.storageengine.api.StoreId.DEFAULT.getRandomId(),
            org.neo4j.storageengine.api.StoreId.DEFAULT.getUpgradeTime(),
            org.neo4j.storageengine.api.StoreId.DEFAULT.getUpgradeId() );

    public static boolean isDefault( StoreId storeId )
    {
        return storeId.getCreationTime() == DEFAULT.getCreationTime() &&
                storeId.getRandomId() == DEFAULT.getRandomId() &&
                storeId.getUpgradeTime() == DEFAULT.getUpgradeTime() &&
                storeId.getUpgradeId() == DEFAULT.getUpgradeId();
    }

    private long creationTime;
    private long randomId;
    private long upgradeTime;
    private long upgradeId;

    public StoreId( org.neo4j.storageengine.api.StoreId kernelStoreId )
    {
        this( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(), kernelStoreId.getUpgradeTime(), kernelStoreId.getUpgradeId() );
    }

    public StoreId( long creationTime, long randomId, long upgradeTime, long upgradeId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.upgradeTime = upgradeTime;
        this.upgradeId = upgradeId;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getRandomId()
    {
        return randomId;
    }

    public long getUpgradeTime()
    {
        return upgradeTime;
    }

    public long getUpgradeId()
    {
        return upgradeId;
    }

    public boolean equalToKernelStoreId( org.neo4j.storageengine.api.StoreId kernelStoreId )
    {
        return creationTime == kernelStoreId.getCreationTime() &&
               randomId == kernelStoreId.getRandomId() &&
               upgradeTime == kernelStoreId.getUpgradeTime() &&
               upgradeId == kernelStoreId.getUpgradeId();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( isDefault( this ) )
        {
            return false;
        }
        StoreId storeId = (StoreId) o;
        if ( isDefault( storeId ) )
        {
            return false;
        }
        return creationTime == storeId.creationTime &&
                randomId == storeId.randomId &&
                upgradeTime == storeId.upgradeTime &&
                upgradeId == storeId.upgradeId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( creationTime, randomId, upgradeTime, upgradeId );
    }

    @Override
    public String toString()
    {
        return format( "Store{creationTime:%d, randomId:%s, upgradeTime:%d, upgradeId:%d}",
                creationTime, randomId, upgradeTime, upgradeId );
    }
}
