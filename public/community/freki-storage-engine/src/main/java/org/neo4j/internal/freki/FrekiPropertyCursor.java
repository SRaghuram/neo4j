/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.freki;

import org.neo4j.internal.freki.generated.PropertyValue;
import org.neo4j.internal.freki.generated.StoreRecord;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class FrekiPropertyCursor implements StoragePropertyCursor
{
    private final Store mainStore;
    private final Record record = new Record( 4 );
    private StoreRecord storeRecord;

    private PageCursor cursor;
    private long nodeId;
    private int propertyIteratorIndex;

    FrekiPropertyCursor( Store mainStore )
    {
        this.mainStore = mainStore;
        reset();
    }

    @Override
    public void initNodeProperties( long nodeId )
    {
        this.nodeId = nodeId;
    }

    @Override
    public void initRelationshipProperties( long reference )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int propertyKey()
    {
        return storeRecord.properties( propertyIteratorIndex ).key();
    }

    @Override
    public ValueGroup propertyType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value propertyValue()
    {
        PropertyValue value = storeRecord.properties( propertyIteratorIndex ).value();
        return MutableNodeRecordData.propertyValue( value );
    }

    @Override
    public boolean next()
    {
        if ( nodeId != -1 || propertyIteratorIndex != -1 )
        {
            if ( nodeId != -1 )
            {
                mainStore.read( cursor(), record, nodeId );
                storeRecord = StoreRecord.getRootAsStoreRecord( record.data );
                nodeId = -1;
                if ( !storeRecord.inUse() )
                {
                    return false;
                }
            }
            propertyIteratorIndex++;
            if ( propertyIteratorIndex >= storeRecord.propertiesLength() )
            {
                propertyIteratorIndex = -1;
                return false;
            }
            return true;
        }
        return false;
    }

    private PageCursor cursor()
    {
        if ( cursor == null )
        {
            cursor = mainStore.openReadCursor();
        }
        return cursor;
    }

    @Override
    public void reset()
    {
        nodeId = -1;
        propertyIteratorIndex = -1;
    }

    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }
    }
}
