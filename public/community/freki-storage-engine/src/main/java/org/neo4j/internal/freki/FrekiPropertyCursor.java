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
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readLongs;

public class FrekiPropertyCursor extends FrekiMainStoreCursor implements StoragePropertyCursor
{
    private boolean initializedFromEntity;

    // either the properties are in the record data, where these fields come into play...
    private FrekiReference referenceToLoad;
    private int[] propertyKeyArray;
    private int propertyKeyIndex;
    private Value readValue;

    // ... or they are in the dense form, where these fields come into play
    private Iterator<StorageProperty> denseProperties;
    private StorageProperty currentDenseProperty;

    public FrekiPropertyCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
    }

    @Override
    public void initNodeProperties( Reference reference )
    {
        reset();
        this.referenceToLoad = (FrekiReference) reference;
    }

    @Override
    public void initNodeProperties( StorageNodeCursor nodeCursor )
    {
        if ( ((FrekiMainStoreCursor) nodeCursor).initializeOtherCursorFromStateOfThisCursor( this ) && readNodePropertyKeys() )
        {
            initializedFromEntity = true;
        }
        else
        {
            reset();
        }
    }

    @Override
    public void initRelationshipProperties( Reference reference )
    {
        reset();
        this.referenceToLoad = (FrekiReference) reference;
    }

    @Override
    public void initRelationshipProperties( StorageRelationshipCursor relationshipCursor )
    {
        if ( relationshipCursor.hasProperties() )
        {
            FrekiRelationshipCursor relCursor = (FrekiRelationshipCursor) relationshipCursor;
            if ( relCursor.initializeOtherCursorFromStateOfThisCursor( this ) )
            {
                if ( headerState.isDense )
                {
                    denseProperties = relCursor.denseProperties();
                    return;
                }
                else
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
        }
        reset();
    }

    @Override
    public int propertyKey()
    {
        return headerState.isDense
               ? currentDenseProperty.propertyKeyId()
               : propertyKeyArray[propertyKeyIndex];
    }

    @Override
    public ValueGroup propertyType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value propertyValue()
    {
        if ( headerState.isDense )
        {
            return currentDenseProperty.value();
        }
        if ( readValue == null )
        {
            readValue = PropertyValueFormat.readEagerly( data, stores.bigPropertyValueStore );
        }
        return readValue;
    }

    @Override
    public boolean next()
    {
        if ( loadedNodeId != NULL && headerState.isDense )
        {
            return nextDense();
        }

        boolean hasReferenceToLoad = referenceToLoad != null && referenceToLoad.sourceNodeId != NULL;
        if ( hasReferenceToLoad || propertyKeyIndex != NULL || initializedFromEntity )
        {
            initializedFromEntity = false;
            if ( hasReferenceToLoad )
            {
                FrekiReference reference = referenceToLoad;
                referenceToLoad = null;

                boolean inUse = loadMainRecord( reference.sourceNodeId );
                if ( !inUse )
                {
                    return false;
                }

                if ( headerState.isDense )
                {
                    // TODO would be nice to not have to load this x1 record if this is a dense reference. Or we could also consider
                    //      always trying to reference a sparse node in our reference if possible (if it's cheaper to lookup).
                    DenseStore.RelationshipData relationship =
                            stores.denseStore.getRelationship( reference.sourceNodeId, reference.type, Direction.OUTGOING, reference.endNodeId,
                                    reference.internalId, cursorTracer );
                    denseProperties = relationship != null ? relationship.properties() : emptyIterator();
                    return nextDense();
                }
                else
                {
                    int offset;
                    if ( !reference.relationship )
                    {
                        // This is properties for a node
                        offset = headerState.nodePropertiesOffset;
                    }
                    else
                    {
                        // This is properties for a relationship
                        offset = findRelationshipPropertiesOffset( reference );
                    }
                    if ( !readPropertyKeys( offset ) )
                    {
                        return false;
                    }
                }
            }

            propertyKeyIndex++;
            if ( propertyKeyIndex >= propertyKeyArray.length )
            {
                propertyKeyIndex = -1;
                return false;
            }
            if ( readValue == null && propertyKeyIndex > 0 )
            {
                // We didn't read the value, which means we'll have to figure out this position ourselves right here
                data.position( data.position() + calculatePropertyValueSizeIncludingTypeHeader( data ) );
            }
            readValue = null;
            return true;
        }
        return false;
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

    private int findRelationshipPropertiesOffset( FrekiReference reference )
    {
        readRelationshipTypesAndOffsets();
        int searchIndex = Arrays.binarySearch( relationshipTypesInNode, reference.type );
        if ( searchIndex >= 0 )
        {
            data.position( relationshipTypeOffset( searchIndex ) );
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
                if ( internalId == reference.internalId )
                {
                    return hasProperties ? relationshipPropertiesOffset( relationshipGroupPropertiesOffset, hasPropertiesIndex ) : NULL_OFFSET;
                }
            }
        }
        return NULL_OFFSET;
    }

    private boolean readNodePropertyKeys()
    {
        if ( headerState.isDense )
        {
            denseProperties = stores.denseStore.getProperties( loadedNodeId, cursorTracer );
            return true;
        }
        else
        {
            return readPropertyKeys( headerState.nodePropertiesOffset );
        }
    }

    private boolean readPropertyKeys( int offset )
    {
        if ( offset == NULL_OFFSET )
        {
            return false;
        }

        data.position( offset );
        propertyKeyArray = readIntDeltas( new StreamVByte.IntArrayTarget(), data ).array();
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
    }
}
