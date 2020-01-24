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

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.internal.freki.MutableNodeRecordData.internalRelationshipIdFromRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.nodeIdFromRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipHasProperties;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readLongs;

class FrekiPropertyCursor extends FrekiMainStoreCursor implements StoragePropertyCursor
{
    private boolean initializedFromEntity;
    private long nodeId;
    private long internalRelationshipId;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
    private int nextPropertyValueStartOffset;

    FrekiPropertyCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
    }

    @Override
    public void initNodeProperties( long reference )
    {
        reset();
        if ( reference != NULL )
        {
            nodeId = reference;
        }
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
        reset();
        if ( reference != NULL )
        {
            nodeId = nodeIdFromRelationshipId( reference );
            internalRelationshipId = internalRelationshipIdFromRelationshipId( reference );
        }
    }

    @Override
    public void initRelationshipProperties( StorageRelationshipCursor relationshipCursor )
    {
        if ( relationshipCursor.hasProperties() )
        {
            FrekiRelationshipCursor relCursor = (FrekiRelationshipCursor) relationshipCursor;
            if ( useSharedRecordFrom( relCursor ) )
            {
                int offset = relCursor.currentRelationshipPropertiesOffset();
                if ( offset != NULL_OFFSET )
                {
                    readPropertyKeys( offset );
                    initializedFromEntity = true;
                    return;
                }
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
        if ( nodeId != NULL || propertyKeyIndex != NULL || initializedFromEntity )
        {
            initializedFromEntity = false;
            if ( nodeId != NULL )
            {
                loadMainRecord( nodeId );
                nodeId = NULL;
                if ( !record.hasFlag( Record.FLAG_IN_USE ) )
                {
                    return false;
                }

                int offset;
                if ( internalRelationshipId == NULL )
                {
                    // This is properties for a node
                    offset = nodePropertiesOffset;
                }
                else
                {
                    // This is properties for a relationship
                    offset = findRelationshipPropertiesOffset( internalRelationshipId );
                }
                if ( !readPropertyKeys( offset ) )
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

    private int findRelationshipPropertiesOffset( long internalRelationshipId )
    {
        readRelationshipTypes();
        for ( int t = 0; t < relationshipTypesInNode.length; t++ )
        {
            data.position( relationshipTypeOffset( t ) );
            int hasPropertiesIndex = -1;
            long[] relationshipGroupData = readLongs( data );
            int relationshipGroupPropertiesOffset = data.position();
            for ( int d = 0; d < relationshipGroupData.length; d += 2 )
            {
                boolean hasProperties = relationshipHasProperties( relationshipGroupData[d] );
                if ( hasProperties )
                {
                    hasPropertiesIndex++;
                }
                long internalId = relationshipGroupData[d + 1];
                if ( internalId == internalRelationshipId )
                {
                    return hasProperties ? relationshipPropertiesOffset( relationshipGroupPropertiesOffset, hasPropertiesIndex ) : NULL_OFFSET;
                }
            }
        }
        return NULL_OFFSET;
    }

    private boolean readNodePropertyKeys()
    {
        return readPropertyKeys( nodePropertiesOffset );
    }

    private boolean readPropertyKeys( int offset )
    {
        if ( offset == NULL_OFFSET )
        {
            return false;
        }

        data.position( offset );
        propertyKeyArray = readIntDeltas( new StreamVByte.IntArrayTarget(), data ).array();
        nextPropertyValueStartOffset = data.position();
        propertyKeyIndex = -1;
        return true;
    }

    @Override
    public void reset()
    {
        super.reset();
        nodeId = NULL;
        internalRelationshipId = NULL;
        propertyKeyIndex = -1;
        initializedFromEntity = false;
    }
}
