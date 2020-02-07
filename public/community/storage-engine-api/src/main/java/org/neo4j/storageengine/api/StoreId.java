/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.MetaDataStoreCommon;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

public final class StoreId
{
    public static final StoreId UNKNOWN = new StoreId( -1, -1, -1, -1, -1 );

    private static final Random r = new SecureRandom();

    private final long creationTime;
    private final long randomId;
    private final long storeVersion;
    private final long upgradeTime;
    private final long upgradeTxId;

    public StoreId( long storeVersion )
    {
        // If creationTime == upgradeTime && randomNumber == upgradeTxId then store has never been upgraded
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();
        this.storeVersion = storeVersion;
        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
        this.upgradeTime = currentTimeMillis;
        this.upgradeTxId = randomLong;
    }

    public StoreId( long creationTime, long randomId, long storeVersion )
    {
        this( creationTime, randomId, storeVersion, creationTime, randomId );
    }

    public StoreId( long creationTime, long randomId, long storeVersion, long upgradeTime, long upgradeTxId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.storeVersion = storeVersion;
        this.upgradeTime = upgradeTime;
        this.upgradeTxId = upgradeTxId;
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

    public long getUpgradeTxId()
    {
        return upgradeTxId;
    }

    public long getStoreVersion()
    {
        return storeVersion;
    }

    public String getStoreVersionString()
    {
        return MetaDataStoreCommon.versionLongToString(storeVersion);
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
        StoreId storeId = (StoreId) o;
        return creationTime == storeId.creationTime &&
               randomId == storeId.randomId &&
               storeVersion == storeId.storeVersion &&
               upgradeTime == storeId.upgradeTime &&
               upgradeTxId == storeId.upgradeTxId;
    }

    public boolean equalsIgnoringUpdate( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        StoreId storeId = (StoreId) o;
        return creationTime == storeId.creationTime && randomId == storeId.randomId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( creationTime, randomId, storeVersion, upgradeTime, upgradeTxId );
    }

    @Override
    public String toString()
    {
        return "StoreId{" +
                "creationTime=" + creationTime +
                ", randomId=" + randomId +
                ", storeVersion=" + storeVersion +
                ", upgradeTime=" + upgradeTime +
                ", upgradeTxId=" + upgradeTxId +
                '}';
    }

    public static StoreId getStoreId(PageCache pageCache, File neoStore ) throws IOException
    {
        return new StoreId(
                getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.TIME ),
                getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.RANDOM_NUMBER ),
                getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.STORE_VERSION ),
                getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.UPGRADE_TIME ),
                getRecord( pageCache, neoStore, MetaDataStoreCommon.Position.UPGRADE_TRANSACTION_ID )
        );
    }

    // this is just to read the storeid
    public static long getRecord(PageCache pageCache, File neoStore, MetaDataStoreCommon.Position position ) throws IOException
    {
        //MetaDataRecordFormat format = new MetaDataRecordFormat();
        int pageSize = MetaDataStoreCommon.getPageSize( pageCache );
        long value = MetaDataStoreCommon.FIELD_NOT_PRESENT;
        int offset = MetaDataStoreCommon.RECORD_SIZE * position.id;;//offset( position );
        try ( PagedFile pagedFile = pageCache.map( neoStore, EmptyVersionContextSupplier.EMPTY, pageSize, immutable.empty()  ) )
        {
            if ( pagedFile.getLastPageId() >= 0 )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, TRACER_SUPPLIER.get() ) )
                {
                    if ( cursor.next() )
                    {
                        cursor.setOffset( offset );
                        byte inUse = cursor.getByte();
                        if ( inUse == MetaDataStoreCommon.Record.IN_USE.byteValue() )
                        {
                            value = cursor.getLong();
                        }
                    }
                }
            }
        }
        return value;
    }
}
