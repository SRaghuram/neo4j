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
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.internal.freki.MutableNodeData.ARRAY_ENTRIES_PER_RELATIONSHIP;
import static org.neo4j.internal.freki.MutableNodeData.externalRelationshipId;
import static org.neo4j.internal.freki.MutableNodeData.otherNodeOf;
import static org.neo4j.internal.freki.MutableNodeData.relationshipHasProperties;
import static org.neo4j.internal.freki.MutableNodeData.relationshipIsOutgoing;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

public class FrekiRelationshipTraversalCursor extends FrekiRelationshipCursor implements StorageRelationshipTraversalCursor
{
    private boolean loadedCorrectNode;
    private long nodeId;
    private RelationshipSelection selection;

    private long[] currentTypeData;
    private int currentTypeIndex;
    private int currentTypeRelationshipIndex;
    private int currentTypePropertiesIndex;
    private int currentTypePropertiesOffset;

    // Accidental state from currentTypeData
    private long currentRelationshipOtherNode;
    private RelationshipDirection currentRelationshipDirection;
    private boolean currentRelationshipHasProperties;
    private long currentRelationshipInternalId;

    // dense node state
    private ResourceIterator<DenseRelationshipStore.RelationshipData> denseRelationships;
    private DenseRelationshipStore.RelationshipData currentDenseRelationship;
    private int selectionCriterionIndex;
    private long neighbourNodeReferenceSelection;

    public FrekiRelationshipTraversalCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer );
    }

    @Override
    public boolean hasProperties()
    {
        if ( currentDenseRelationship != null )
        {
            return denseProperties().hasNext();
        }
        return currentRelationshipHasProperties;
    }

    @Override
    public Reference propertiesReference()
    {
        return currentRelationshipHasProperties ? relationshipReference() : FrekiReference.NULL_REFERENCE;
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        if ( !hasProperties() )
        {
            propertyCursor.reset();
            return;
        }

        FrekiPropertyCursor frekiPropertyCursor = (FrekiPropertyCursor) propertyCursor;
        frekiPropertyCursor.initRelationshipProperties( this );
    }

    @Override
    int currentRelationshipPropertiesOffset()
    {
        return relationshipPropertiesOffset( data.relationshipBuffer(), currentTypePropertiesOffset, currentTypePropertiesIndex );
    }

    @Override
    Iterator<StorageProperty> denseProperties()
    {
        if ( densePropertiesItr == null )
        {
            densePropertiesItr = currentDenseRelationship.properties();
        }
        return densePropertiesItr;
    }

    @Override
    public long entityReference()
    {
        return externalRelationshipId( data.nodeId, currentRelationshipInternalId, currentRelationshipOtherNode, currentRelationshipDirection.isOutgoing() );
    }

    @Override
    public Reference relationshipReference()
    {
        return FrekiReference.relationshipReference( sourceNodeReference(), currentRelationshipInternalId, type(),
                currentDirection() == LOOP ? LOOP : OUTGOING, targetNodeReference() );
    }

    @Override
    public boolean next()
    {
        if ( selection == null )
        {
            // TODO this is a clear sign that this cursor hasn't been initialized. Ideally we should not get here ever, but as long as kernel cursors
            //      do the wrong thing now and then we can play nice and return false here.
            return false;
        }
        if ( !loadedCorrectNode )
        {
            if ( !load( nodeId ) )
            {
                return false;
            }
            ensureRelationshipsLoaded();
            if ( !data.isDense && data.relationshipOffset == 0 )
            {
                return false;
            }

            startIterationAfterLoad();
            readRelationshipTypesAndOffsets();
        }

        if ( data.isDense )
        {
            // TODO We could be clever and place a type[] in the quick access record so that we know which types even exist for this node
            //      if we do this we don't have to make a tree seek for every relationship type when there will be nothing there
            while ( selectionCriterionIndex < selection.numberOfCriteria() )
            {
                if ( denseRelationships == null )
                {
                    RelationshipSelection.Criterion criterion = selection.criterion( selectionCriterionIndex );
                    denseRelationships = neighbourNodeReferenceSelection == NULL
                            ? stores.denseStore.getRelationships( data.nodeId, criterion.type(), criterion.direction(), cursorTracer )
                            : stores.denseStore.getRelationships( data.nodeId, criterion.type(), criterion.direction(), neighbourNodeReferenceSelection,
                                    cursorTracer );
                }

                if ( denseRelationships.hasNext() )
                {
                    // We don't need filtering here because we ask for the correct type and direction right away in the tree seek
                    currentDenseRelationship = denseRelationships.next();
                    currentRelationshipDirection = currentDenseRelationship.direction();
                    currentRelationshipInternalId = currentDenseRelationship.internalId();
                    currentRelationshipOtherNode = currentDenseRelationship.neighbourNodeId();
                    currentRelationshipHasProperties = currentDenseRelationship.hasProperties();
                    densePropertiesItr = null;
                    return true;
                }
                // Mark the end of this criterion
                denseRelationships.close();
                denseRelationships = null;
                selectionCriterionIndex++;
            }
        }
        else
        {
            while ( currentTypeIndex < relationshipTypesInNode.length )
            {
                if ( currentTypeRelationshipIndex == -1 )
                {
                    // Time to load data for the next type
                    int candidateType = relationshipTypesInNode[currentTypeIndex];
                    if ( !selection.test( candidateType ) )
                    {
                        // Skip this type completely, it wasn't part of the requested selection
                        currentTypeIndex++;
                        continue;
                    }

                    ByteBuffer relationshipBuffer = data.relationshipBuffer( relationshipTypeOffset( currentTypeIndex ) );
                    currentTypeData = readLongs( relationshipBuffer );
                    currentTypePropertiesOffset = relationshipBuffer.position();
                    currentTypeRelationshipIndex = 0;
                    currentTypePropertiesIndex = -1;
                }

                if ( sparseNextFromCurrentType() )
                {
                    return true;
                }
                sparseGoToNextType();
            }
        }
        dereferenceData();
        selection = null;
        return false;
    }

    private void sparseGoToNextType()
    {
        currentTypeIndex++;
        currentTypeRelationshipIndex = -1;
    }

    private boolean sparseNextFromCurrentType()
    {
        while ( currentTypeRelationshipIndex * ARRAY_ENTRIES_PER_RELATIONSHIP < currentTypeData.length )
        {
            int index = currentTypeRelationshipIndex++;
            int dataArrayIndex = index * ARRAY_ENTRIES_PER_RELATIONSHIP; // because of two longs per relationship
            long currentRelationshipOtherNodeRaw = currentTypeData[dataArrayIndex];
            currentRelationshipInternalId = currentTypeData[dataArrayIndex + 1];
            currentRelationshipOtherNode = otherNodeOf( currentRelationshipOtherNodeRaw );
            if ( currentRelationshipOtherNode == nodeId )
            {
                currentRelationshipDirection = LOOP;
            }
            else
            {
                boolean outgoing = relationshipIsOutgoing( currentRelationshipOtherNodeRaw );
                currentRelationshipDirection = outgoing ? OUTGOING : INCOMING;
            }
            currentRelationshipHasProperties = relationshipHasProperties( currentRelationshipOtherNodeRaw );
            if ( currentRelationshipHasProperties )
            {
                currentTypePropertiesIndex++;
            }

            // TODO a thought about this filtering. It may be beneficial to order the relationships of: OUTGOING,LOOP,INCOMING
            //      so that filtering OUTGOING/INCOMING would basically then be to find the point where to stop, instead of going
            //      through all relationships of that type. This may also require an addition to RelationshipSelection so that
            //      it can be asked about requested direction.
            if ( selection.test( relationshipTypesInNode[currentTypeIndex], currentRelationshipDirection ) &&
                    (neighbourNodeReferenceSelection == NULL || neighbourNodeReferenceSelection == currentRelationshipOtherNode) )
            {
                return true;
            }
        }
        return false;
    }

    private void startIterationAfterLoad()
    {
        loadedCorrectNode = true;
        currentTypeIndex = 0;
        selectionCriterionIndex = 0;
    }

    @Override
    public void reset()
    {
        super.reset();
        nodeId = NULL;
        selection = null;
        selectionCriterionIndex = -1;
        currentTypeIndex = -1;
        currentTypeRelationshipIndex = -1;
        loadedCorrectNode = false;
        currentRelationshipOtherNode = NULL;
        currentRelationshipHasProperties = false;
        currentTypePropertiesIndex = -1;
        currentTypePropertiesOffset = -1;
        currentRelationshipDirection = null;
        currentRelationshipInternalId = NULL;
        if ( denseRelationships != null )
        {
            denseRelationships.close();
            denseRelationships = null;
        }
        currentDenseRelationship = null;
        densePropertiesItr = null;
        neighbourNodeReferenceSelection = NULL;
    }

    @Override
    public int type()
    {
        return currentDenseRelationship != null
               ? currentDenseRelationship.type()
               : relationshipTypesInNode[currentTypeIndex];
    }

    @Override
    public long sourceNodeReference()
    {
        if ( currentRelationshipDirection == OUTGOING )
        {
            return data.nodeId;
        }
        return currentDenseRelationship != null
               ? currentDenseRelationship.neighbourNodeId()
               : currentRelationshipOtherNode;
    }

    @Override
    public long targetNodeReference()
    {
        if ( currentRelationshipDirection == INCOMING )
        {
            return data.nodeId;
        }
        return currentDenseRelationship != null
               ? currentDenseRelationship.neighbourNodeId()
               : currentRelationshipOtherNode;
    }

    @Override
    public long neighbourNodeReference()
    {
        return currentDenseRelationship != null
               ? currentDenseRelationship.neighbourNodeId()
               : currentRelationshipOtherNode;
    }

    @Override
    public long originNodeReference()
    {
        return nodeId;
    }

    RelationshipDirection currentDirection()
    {
        return currentDenseRelationship != null
                ? currentDenseRelationship.direction()
                : currentRelationshipDirection;
    }

    @Override
    public void init( long nodeId, long reference, RelationshipSelection selection )
    {
        cursorAccessTracer.registerNodeToRelationshipsByReference( nodeId );
        //We don't use reference, is that a problem?
        initInternal( nodeId, NULL, selection );
    }

    void initInternal( long nodeId, long neighbourNodeReferenceSelection, RelationshipSelection selection )
    {
        reset();
        this.nodeId = nodeId;
        this.neighbourNodeReferenceSelection = neighbourNodeReferenceSelection;
        this.selection = selection;
    }

    @Override
    public void init( StorageNodeCursor nodeCursor, RelationshipSelection selection )
    {
        cursorAccessTracer.registerNodeToRelationshipsDirect();
        initInternal( nodeCursor.entityReference(), NULL, selection );
        if ( ((FrekiMainStoreCursor) nodeCursor).initializeOtherCursorFromStateOfThisCursor( this ) )
        {
            startIterationAfterLoad();
            readRelationshipTypesAndOffsets();
        }
        else
        {
            reset();
        }
    }

    void init( StorageNodeCursor nodeCursor, RelationshipSelection selection, long neighbourNodeReference )
    {
        init( nodeCursor, selection );
        this.neighbourNodeReferenceSelection = neighbourNodeReference;
    }

    void init( StorageNodeCursor nodeCursor, RelationshipSelection selection, long neighbourNodeReference )
    {
        init( nodeCursor, selection );
        this.neighbourNodeReferenceSelection = neighbourNodeReference;
    }
}
