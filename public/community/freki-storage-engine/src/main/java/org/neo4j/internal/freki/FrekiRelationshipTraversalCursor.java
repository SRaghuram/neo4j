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

import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.internal.freki.MutableNodeRecordData.ARRAY_ENTRIES_PER_RELATIONSHIP;
import static org.neo4j.internal.freki.MutableNodeRecordData.externalRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.otherNodeOf;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipHasProperties;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipIsOutgoing;
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
    private PrefetchingResourceIterator<DenseStore.RelationshipData> denseRelationships;

    FrekiRelationshipTraversalCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
    }

    @Override
    public boolean hasProperties()
    {
        return currentRelationshipHasProperties;
    }

    @Override
    public long propertiesReference()
    {
        return currentRelationshipHasProperties ? entityReference() : NULL;
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
        return relationshipPropertiesOffset( currentTypePropertiesOffset, currentTypePropertiesIndex );
    }

    @Override
    public long entityReference()
    {
        return externalRelationshipId( loadedNodeId, currentRelationshipInternalId, currentRelationshipOtherNode, currentRelationshipDirection.isOutgoing() );
    }

    @Override
    public boolean next()
    {
        if ( !loadedCorrectNode )
        {
            if ( !loadMainRecord( nodeId ) || (!isDense && relationshipsOffset == 0) )
            {
                return false;
            }

            if ( isDense )
            {

            }
            else
            {
                startIterationAfterLoad();
                readRelationshipTypes();
            }
        }

        while ( currentTypeIndex < relationshipTypesInNode.length )
        {
            if ( currentTypeRelationshipIndex == -1 )
            {
                // Time to move over to the next type
                int candidateType = relationshipTypesInNode[currentTypeIndex];
                if ( !selection.test( candidateType ) )
                {
                    // Skip this type completely, it wasn't part of the requested selection
                    currentTypeIndex++;
                    continue;
                }

                currentTypeData = StreamVByte.readLongs( data );
                currentTypePropertiesOffset = data.position();
                currentTypeRelationshipIndex = 0;
                currentTypePropertiesIndex = -1;
            }

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
                if ( selection.test( relationshipTypesInNode[currentTypeIndex], currentRelationshipDirection ) )
                {
                    return true;
                }
            }
            currentTypeIndex++;
            currentTypeRelationshipIndex = -1;
            if ( currentTypePropertiesIndex >= 0 && currentTypeIndex < relationshipTypesInNode.length )
            {
                //Skipping the properties
                data.position( relationshipTypeOffset( currentTypeIndex ) );
            }
        }
        return false;
    }

    private void startIterationAfterLoad()
    {
        loadedCorrectNode = true;
        currentTypeIndex = 0;
    }

    @Override
    public void reset()
    {
        super.reset();
        nodeId = NULL;
        selection = null;
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
    }

    @Override
    public int type()
    {
        return relationshipTypesInNode[currentTypeIndex];
    }

    @Override
    public long sourceNodeReference()
    {
        return currentRelationshipDirection == OUTGOING ? loadedNodeId : currentRelationshipOtherNode;
    }

    @Override
    public long targetNodeReference()
    {
        return currentRelationshipDirection == INCOMING ? loadedNodeId : currentRelationshipOtherNode;
    }

    @Override
    public long neighbourNodeReference()
    {
        return currentRelationshipOtherNode;
    }

    @Override
    public long originNodeReference()
    {
        return nodeId;
    }

    RelationshipDirection currentDirection()
    {
        return currentRelationshipDirection;
    }

    @Override
    public void init( long nodeId, long reference, RelationshipSelection selection )
    {
        reset();
        this.nodeId = nodeId;
        this.selection = selection;
    }

    @Override
    public void init( StorageNodeCursor nodeCursor, RelationshipSelection selection )
    {
        init( nodeCursor.entityReference(), nodeCursor.relationshipsReference(), selection );
        useSharedRecordFrom( (FrekiNodeCursor) nodeCursor );
        startIterationAfterLoad();
        readRelationshipTypes();
    }
}
