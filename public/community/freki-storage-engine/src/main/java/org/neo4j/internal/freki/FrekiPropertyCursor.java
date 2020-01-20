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
package org.neo4j.internal.freki;

import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;

class FrekiPropertyCursor extends FrekiMainStoreCursor implements StoragePropertyCursor
{
    private boolean initializedFromEntity;
    private long entityId;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
    private int nextPropertyValueStartOffset;

    FrekiPropertyCursor( Store mainStore )
    {
        super( mainStore );
    }

    @Override
    public void initNodeProperties( long nodeId )
    {
        // Setting this field will tell make record loading happen in next() later
        this.entityId = nodeId;
        reset();
    }

    @Override
    public void initNodeProperties( StorageNodeCursor nodeCursor )
    {
        if ( useSharedRecordFrom( (FrekiNodeCursor) nodeCursor ) && readNodePropertyKeys() )
        {
            initializedFromEntity = true;
        }
        else
        {
            reset();
        }
    }

    @Override
    public void initRelationshipProperties( long reference )
    {
    }

    @Override
    public void initRelationshipProperties( StorageRelationshipCursor relationshipCursor )
    {
        FrekiRelationshipCursor relCursor = (FrekiRelationshipCursor) relationshipCursor;
        if ( useSharedRecordFrom( relCursor ) )
        {
            int offset = relCursor.currentRelationshipPropertiesOffset();
            if ( offset != -1 )
            {
                readPropertyKeys( offset );
                initializedFromEntity = true;
                return;
            }
        }
        reset();
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
        return PropertyValueFormat.read( data );
    }

    @Override
    public boolean next()
    {
        if ( entityId != -1 || propertyKeyIndex != -1 || initializedFromEntity )
        {
            initializedFromEntity = false;
            if ( entityId != -1 )
            {
                loadMainRecord( entityId );
                entityId = -1;
                if ( !record.hasFlag( Record.FLAG_IN_USE ) || !readNodePropertyKeys() )
                {
                    return false;
                }
            }

            propertyKeyIndex++;
            if ( propertyKeyIndex >= propertyKeyArray.length )
            {
                propertyKeyIndex = -1;
                return false;
            }
            data.position( nextPropertyValueStartOffset );

            // Calculate the position in the buffer where the next value will be.
            // If we decide to store an offset array along with the key array too then this becomes easier where we don't have to calculate it by hand.
            nextPropertyValueStartOffset += calculatePropertyValueSizeIncludingTypeHeader( data );
            return true;
        }
        return false;
    }

    private boolean readNodePropertyKeys()
    {
        return readPropertyKeys( propertiesOffset );
    }

    private boolean readPropertyKeys( int offset )
    {
        if ( offset == 0 )
        {
            return false;
        }

        data.position( offset );
        propertyKeyArray = readIntDeltas( new StreamVByte.IntArrayTarget(), data ).array();
        nextPropertyValueStartOffset = data.position();
        return true;
    }

    @Override
    public void reset()
    {
        super.reset();
        entityId = -1;
        propertyKeyIndex = -1;
        initializedFromEntity = false;
    }
}
