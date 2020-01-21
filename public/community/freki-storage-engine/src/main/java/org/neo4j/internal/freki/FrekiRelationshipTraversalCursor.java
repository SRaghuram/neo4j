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

import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;
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
    private int expectedType;
    private RelationshipDirection expectedDirection;

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

    FrekiRelationshipTraversalCursor( SimpleStore mainStore )
    {
        super( mainStore );
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
        return externalRelationshipId( record.id, currentRelationshipInternalId, currentRelationshipOtherNode, currentRelationshipDirection.isOutgoing() );
    }

    @Override
    public boolean next()
    {
        if ( !loadedCorrectNode )
        {
            if ( !loadMainRecord( nodeId ) || relationshipsOffset == 0 )
            {
                return false;
            }

            startIterationAfterLoad();
            readRelationshipTypes();
            if ( expectedType != NULL )
            {
                int foundIndex = Arrays.binarySearch( relationshipTypesInNode, expectedType );
                if ( foundIndex < 0 )
                {
                    return false;
                }
                currentTypeIndex = foundIndex;
                data.position( relationshipTypeOffset( foundIndex ) );
            }
        }

        while ( currentTypeIndex < relationshipTypesInNode.length )
        {
            if ( currentTypeRelationshipIndex == -1 )
            {
                if ( (expectedType != NULL && relationshipTypesInNode[currentTypeIndex] != expectedType) )
                {
                    break;
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

                boolean matchesDirection = expectedDirection == null || currentRelationshipDirection.equals( expectedDirection );
                if ( matchesDirection )
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
        expectedType = -1;
        expectedDirection = null;
        currentTypeIndex = -1;
        currentTypeRelationshipIndex = -1;
        loadedCorrectNode = false;
        currentRelationshipOtherNode = NULL;
        currentRelationshipHasProperties = false;
        currentTypePropertiesIndex = -1;
        currentTypePropertiesOffset = -1;
        currentRelationshipDirection = null;
        currentRelationshipInternalId = NULL;
    }

    @Override
    public int type()
    {
        return relationshipTypesInNode[currentTypeIndex];
    }

    @Override
    public long sourceNodeReference()
    {
        return currentRelationshipDirection == OUTGOING ? record.id : currentRelationshipOtherNode;
    }

    @Override
    public long targetNodeReference()
    {
        return currentRelationshipDirection == INCOMING ? record.id : currentRelationshipOtherNode;
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

    @Override
    public void init( long nodeId, long reference, boolean nodeIsDense )
    {
        reset();
        this.nodeId = nodeId;
    }

    @Override
    public void init( StorageNodeCursor nodeCursor )
    {
        init( nodeCursor.entityReference(), nodeCursor.allRelationshipsReference(), nodeCursor.isDense() );
        useSharedRecordFrom( (FrekiNodeCursor) nodeCursor );
        startIterationAfterLoad();
        readRelationshipTypes();
    }

    @Override
    public void init( long nodeId, long reference, int type, RelationshipDirection direction, boolean nodeIsDense )
    {
        init( nodeId, reference, nodeIsDense );
        this.expectedType = type;
        this.expectedDirection = direction;
    }

    @Override
    public void init( StorageRelationshipGroupCursor groupCursor, long reference, int type, RelationshipDirection direction, boolean nodeIsDense )
    {
        init( groupCursor.getOwningNode(), reference, nodeIsDense );
        useSharedRecordFrom( (FrekiRelationshipGroupCursor) groupCursor );
        startIterationAfterLoad();
        expectedType = type;
        expectedDirection = direction;

        // Here we come from a place where the relationship data is already loaded in the FrekiRelationshipGroupCursor so we don't have to
        // read it again... merely initialize the state in here to make it look like it has one group (this type) and let next() enjoy this data.
        // The reference is the data buffer offset, so just go there.
        relationshipTypesInNode = new int[]{type};
        data.position( (int) reference );
        currentTypeIndex = 0;
    }
}
