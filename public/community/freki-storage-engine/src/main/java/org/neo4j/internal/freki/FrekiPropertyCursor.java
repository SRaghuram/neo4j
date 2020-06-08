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
<<<<<<< HEAD
=======
import org.neo4j.memory.MemoryTracker;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Collections.emptyIterator;
<<<<<<< HEAD
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipHasProperties;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.PropertyValueFormat.readEagerly;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
=======
import static org.neo4j.internal.freki.MutableNodeData.relationshipHasProperties;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.PropertyValueFormat.readEagerly;
import static org.neo4j.internal.freki.StreamVByte.readInts;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.neo4j.internal.freki.StreamVByte.readLongs;

class FrekiPropertyCursor extends FrekiMainStoreCursor implements StoragePropertyCursor
{
    private boolean initializedFromEntity;

<<<<<<< HEAD
    // either the properties are in the record data, where these fields come into play...
    private FrekiReference referenceToLoad;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
=======
    // either the properties are in the record data (node properties (and relationship properties when sparse)), where these fields come into play...
    private FrekiReference referenceToLoad;
    private int[] propertyKeyArray;
    private int propertyKeyIndex = (int) NULL;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    private int nextValuePosition;
    private Value readValue;
    private ByteBuffer buffer;

<<<<<<< HEAD
    // ... or they are in the dense form, where these fields come into play
    private Iterator<StorageProperty> denseProperties;
    private StorageProperty currentDenseProperty;

    FrekiPropertyCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer );
=======
    // ... or they are in the dense form (relationship properties when dense), where these fields come into play
    private Iterator<StorageProperty> denseProperties;
    private StorageProperty currentDenseProperty;

    FrekiPropertyCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
        if ( ((FrekiMainStoreCursor) nodeCursor).initializeOtherCursorFromStateOfThisCursor( this ) )
        {
            ensurePropertiesLoaded();
=======
        FrekiMainStoreCursor frekiNodeCursor = (FrekiMainStoreCursor) nodeCursor;
        frekiNodeCursor.ensurePropertiesLocated();
        if ( frekiNodeCursor.initializeOtherCursorFromStateOfThisCursor( this ) )
        {
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
                        ensurePropertiesLoaded();
=======
                        ensurePropertiesLocated();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
            readValue = readEagerly( buffer.position( nextValuePosition ), stores.bigPropertyValueStore, cursorTracer );
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

<<<<<<< HEAD
            propertyKeyIndex++;
            if ( propertyKeyIndex >= propertyKeyArray.length )
=======
            boolean hasNextProperty = ++propertyKeyIndex < propertyKeyArray.length;
            if ( !hasNextProperty && data.propertySplitState != null )
            {
                if ( !data.propertySplitState.last )
                {
                    SPLIT_PIECE_LOAD_STATUS status = loadNextSplitPiece( Header.OFFSET_PROPERTIES, data.propertySplitState );
                    if ( status != SPLIT_PIECE_LOAD_STATUS.OK )
                    {
                        throw new IllegalStateException( "Failed to load split property part chain (Retry not yet implemented). " + status.name() );
                    }
                    readPropertyKeys( buffer = data.propertySplitState.buffer );

                    hasNextProperty = true;
                    assert propertyKeyArray.length > 0;
                    propertyKeyIndex++;
                }
                else
                {
                    data.propertySplitState.reset();
                }
            }

            if ( !hasNextProperty )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
            ensurePropertiesLoaded();
            buffer = data.propertyBuffer();
=======
            ensurePropertiesLocated();
            buffer = data.propertyBuffer();
            if ( data.propertySplitState != null )
            {
                data.propertySplitState.reset();
            }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
        propertyKeyArray = readIntDeltas( new StreamVByte.IntArrayTarget(), buffer ).array();
=======
        propertyKeyArray = readInts( buffer, true );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
