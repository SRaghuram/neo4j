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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Collections.emptyIterator;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipHasProperties;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.PropertyValueFormat.readEagerly;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readLongs;

class FrekiPropertyCursor extends FrekiMainStoreCursor implements StoragePropertyCursor
{
    private boolean initializedFromEntity;

    // either the properties are in the record data, where these fields come into play...
    private FrekiReference referenceToLoad;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
    private int nextValuePosition;
    private Value readValue;
    private ByteBuffer buffer;

    // ... or they are in the dense form, where these fields come into play
    private Iterator<StorageProperty> denseProperties;
    private StorageProperty currentDenseProperty;

    FrekiPropertyCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer );
    }

    @Override
    public void initNodeProperties( Reference reference )
    {
        cursorAccessTracer.registerNodeToPropertyByReference( ((FrekiReference) reference).sourceNodeId );
        reset();
        this.referenceToLoad = (FrekiReference) reference;
    }

    @Override
    public void initNodeProperties( StorageNodeCursor nodeCursor )
    {
        cursorAccessTracer.registerNodeToPropertyDirect();
        if ( ((FrekiMainStoreCursor) nodeCursor).initializeOtherCursorFromStateOfThisCursor( this ) )
        {
            ensurePropertiesLoaded();
            buffer = data.propertyBuffer();
            if ( readPropertyKeys( buffer ) )
            {
                initializedFromEntity = true;
                return;
            }
        }
        reset();
    }

    @Override
    public void initRelationshipProperties( Reference reference )
    {
        cursorAccessTracer.registerRelationshipToPropertyByReference( ((FrekiReference) reference).sourceNodeId );
        reset();
        this.referenceToLoad = (FrekiReference) reference;
    }

    @Override
    public void initRelationshipProperties( StorageRelationshipCursor relationshipCursor )
    {
        cursorAccessTracer.registerRelationshipToPropertyDirect();
        if ( relationshipCursor.hasProperties() )
        {
            FrekiRelationshipCursor relCursor = (FrekiRelationshipCursor) relationshipCursor;
            if ( relCursor.initializeOtherCursorFromStateOfThisCursor( this ) )
            {
                if ( data.isDense )
                {
                    denseProperties = relCursor.denseProperties();
                    dereferenceData(); // dereference right away, because we won't be needing that data anymore
                    return;
                }
                else
                {
                    buffer = data.relationshipBuffer();
                    int offset = relCursor.currentRelationshipPropertiesOffset();
                    if ( offset != NULL_OFFSET )
                    {
                        ensurePropertiesLoaded();
                        readPropertyKeys( buffer.position( offset ) );
                        initializedFromEntity = true;
                        return;
                    }
                }
            }
        }
        reset();
    }

    @Override
    public int propertyKey()
    {
        return denseProperties != null
               ? currentDenseProperty.propertyKeyId()
               : propertyKeyArray[propertyKeyIndex];
    }

    @Override
    public ValueGroup propertyType()
    {
        // TODO figure this out w/o deserializing the value
        return propertyValue().valueGroup();
    }

    @Override
    public Value propertyValue()
    {
        if ( denseProperties != null )
        {
            return currentDenseProperty.value();
        }
        if ( readValue == null )
        {
            readValue = readEagerly( buffer.position( nextValuePosition ), stores.bigPropertyValueStore );
            nextValuePosition = buffer.position();
        }
        return readValue;
    }

    @Override
    public boolean next()
    {
        if ( denseProperties != null )
        {
            return nextDense();
        }

        boolean hasReferenceToLoad = referenceToLoad != null && referenceToLoad.sourceNodeId != NULL;
        if ( hasReferenceToLoad || propertyKeyIndex != NULL || initializedFromEntity )
        {
            initializedFromEntity = false;
            if ( hasReferenceToLoad )
            {
                if ( !loadReference() )
                {
                    return false;
                }
            }

            propertyKeyIndex++;
            if ( propertyKeyIndex >= propertyKeyArray.length )
            {
                propertyKeyIndex = -1;
            }
            else
            {
                if ( readValue == null && propertyKeyIndex > 0 )
                {
                    // We didn't read the value, which means we'll have to figure out the position of the next value ourselves right here
                    nextValuePosition += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( nextValuePosition ) );
                }
                readValue = null;
                return true;
            }
        }
        dereferenceData();
        return false;
    }

    private boolean loadReference()
    {
        FrekiReference reference = referenceToLoad;
        referenceToLoad = null;

        boolean inUse = load( reference.sourceNodeId );
        if ( !inUse )
        {
            return false;
        }

        if ( reference.relationship )
        {
            if ( data.isDense )
            {
                DenseRelationshipStore.RelationshipData denseRelationship =
                        stores.denseStore.getRelationship( reference.sourceNodeId, reference.type, Direction.OUTGOING, reference.endNodeId,
                                reference.internalId, cursorTracer );
                denseProperties = denseRelationship != null ? denseRelationship.properties() : emptyIterator();
                return nextDense();
            }
            buffer = data.relationshipBuffer();
            if ( !placeAtRelationshipPropertiesOffset( buffer, reference ) )
            {
                return false;
            }
        }
        else
        {
            // This is properties for a node
            ensurePropertiesLoaded();
            buffer = data.propertyBuffer();
        }
        return readPropertyKeys( buffer );
    }

    public boolean nextDense()
    {
        if ( denseProperties.hasNext() )
        {
            currentDenseProperty = denseProperties.next();
            return true;
        }
        return false;
    }

    private boolean placeAtRelationshipPropertiesOffset( ByteBuffer buffer, FrekiReference reference )
    {
        readRelationshipTypesAndOffsets();
        int searchIndex = Arrays.binarySearch( relationshipTypesInNode, reference.type );
        if ( searchIndex >= 0 )
        {
            buffer.position( relationshipTypeOffset( searchIndex ) );
            int hasPropertiesIndex = -1;
            long[] relationshipGroupData = readLongs( buffer );
            int relationshipGroupPropertiesOffset = buffer.position();
            for ( int d = 0; d < relationshipGroupData.length; d += 2 )
            {
                boolean hasProperties = relationshipHasProperties( relationshipGroupData[d] );
                if ( hasProperties )
                {
                    hasPropertiesIndex++;
                }
                long internalId = relationshipGroupData[d + 1];
                if ( internalId == reference.internalId )
                {
                    int offset = hasProperties ? relationshipPropertiesOffset( buffer, relationshipGroupPropertiesOffset, hasPropertiesIndex ) : NULL_OFFSET;
                    if ( offset != NULL_OFFSET )
                    {
                        buffer.position( offset );
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean readPropertyKeys( ByteBuffer buffer )
    {
        denseProperties = null;
        if ( buffer == null )
        {
            return false;
        }

        propertyKeyArray = readIntDeltas( new StreamVByte.IntArrayTarget(), buffer ).array();
        nextValuePosition = buffer.position();
        propertyKeyIndex = -1;
        return true;
    }

    @Override
    public void reset()
    {
        super.reset();
        referenceToLoad = null;
        propertyKeyIndex = -1;
        initializedFromEntity = false;
        denseProperties = null;
        readValue = null;
        buffer = null;
        currentDenseProperty = null;
    }
}
