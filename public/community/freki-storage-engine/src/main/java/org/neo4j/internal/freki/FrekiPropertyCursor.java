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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.StreamVByte.readDeltas;

class FrekiPropertyCursor implements StoragePropertyCursor
{
    private final Store mainStore;
    private final Record record = new Record( 1 );

    private PageCursor cursor;
    private long nodeId;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
    private int nextPropertyValueStartOffset;

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
        return propertyKeyArray[propertyKeyIndex];
    }

    @Override
    public ValueGroup propertyType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value propertyValue()
    {
        return PropertyValueFormat.read( record.data );
    }

    @Override
    public boolean next()
    {
        if ( nodeId != -1 || propertyKeyIndex != -1 )
        {
            if ( nodeId != -1 )
            {
                mainStore.read( cursor(), record, nodeId );
                nodeId = -1;
                if ( !record.hasFlag( Record.FLAG_IN_USE ) )
                {
                    return false;
                }

                // Read property offset
                int offsetsHeader = MutableNodeRecordData.readOffsetsHeader( record.data );
                int propertyOffset = MutableNodeRecordData.propertyOffset( offsetsHeader );

                // Read property keys
                StreamVByte.IntArrayTarget target = new StreamVByte.IntArrayTarget();
                nextPropertyValueStartOffset = readDeltas( target, record.data.array(), propertyOffset );
                propertyKeyArray = target.array();
            }

            propertyKeyIndex++;
            if ( propertyKeyIndex >= propertyKeyArray.length )
            {
                propertyKeyIndex = -1;
                return false;
            }
            record.data.position( nextPropertyValueStartOffset );

            // Calculate the position in the buffer where the next value will be.
            // If we decide to store an offset array along with the key array too then this becomes easier where we don't have to calculate it by hand.
            nextPropertyValueStartOffset += calculatePropertyValueSizeIncludingTypeHeader( record.data );
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
        propertyKeyIndex = -1;
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
