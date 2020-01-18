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

import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

public class FrekiRelationshipTraversalCursor extends FrekiMainStoreCursor implements StorageRelationshipTraversalCursor
{
    private boolean loadedCorrectNode;
    private long nodeId;
    private int expectedType;
    private RelationshipDirection expectedDirection;

    private int[] typesInNode;
    private int[] typeOffsets;
    private long[] currentTypeData;
    private int currentTypeIndex;
    private int currentTypeDataIndex;
    private int currentTypePropertiesIndex;
    private long currentTypePropertiesOffset;

    // accidental state
    private long currentRelationshipOtherNode;
    private RelationshipDirection currentRelationshipDirection;
    private boolean currentRelationshipHasProperties;
    private int currentRelationshipPropertiesIndex; //Index in types property list where current

    FrekiRelationshipTraversalCursor( Store mainStore )
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
        return -1;
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        long offsetInPropCursor = currentTypePropertiesOffset;
        long indexInTypePropertiesForCurrentRelationship = currentTypePropertiesIndex;
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long entityReference()
    {
        // TODO implement when we have internal relationship IDs (coupled with source node this is yey)
        return 0;
    }

    @Override
    public boolean next()
    {
        if ( !loadedCorrectNode )
        {
            loadMainRecord( nodeId );
            if ( !readHeader() )
            {
                return false;
            }
            readRelationshipTypeGroups();
            if ( expectedType != -1 )
            {
                int foundIndex = Arrays.binarySearch( typesInNode, expectedType );
                if ( foundIndex < 0 )
                {
                    return false;
                }
                currentTypeIndex = foundIndex - 1;
                data.position( typeOffsets[currentTypeIndex] );
            }
        }

        while ( true )
        {
            if ( currentTypeDataIndex == -1 || currentTypeDataIndex >= currentTypeData.length )
            {
                currentTypeIndex++;
                if ( (expectedType != -1 && typesInNode[currentTypeIndex] != expectedType) || currentTypeIndex >= typesInNode.length )
                {
                    break;
                }

                currentTypeData = StreamVByte.readLongs( data );
                currentTypePropertiesOffset = data.position();
                currentTypeDataIndex = 0;
                currentTypePropertiesIndex = 0;
            }

            while ( currentTypeDataIndex < currentTypeData.length )
            {
                long currentRelationshipOtherNodeRaw = currentTypeData[currentTypeDataIndex++];
                currentRelationshipOtherNode = currentRelationshipOtherNodeRaw >>> 2;
                if ( currentRelationshipOtherNode == nodeId )
                {
                    currentRelationshipDirection = LOOP;
                }
                else
                {
                    boolean outgoing = (currentRelationshipOtherNodeRaw & 0b10) != 0;
                    currentRelationshipDirection = outgoing ? OUTGOING : INCOMING;
                }
                currentRelationshipHasProperties = (currentRelationshipOtherNodeRaw & 0b01) != 0;
                if ( currentRelationshipHasProperties )
                {
                    currentRelationshipPropertiesIndex = currentTypePropertiesIndex;
                    currentTypePropertiesIndex++;
                }

                boolean matchesDirection = expectedDirection == null || currentRelationshipDirection.equals( expectedDirection );
                if ( matchesDirection )
                {
                    return true;
                }
            }
            if ( currentTypePropertiesIndex > 0 && currentTypeIndex < typesInNode.length )
            {
                //Skipping the properties
                data.position( typeOffsets[currentTypeIndex] );
            }
        }
        return false;
    }

    private boolean readHeader()
    {
        loadedCorrectNode = true;
        currentTypeIndex = -1;
        return !record.hasFlag( FLAG_IN_USE ) && relationshipsOffset > 0;
    }

    private void readRelationshipTypeGroups()
    {
        data.position( relationshipsOffset );
        typesInNode = StreamVByte.readIntDeltas( new StreamVByte.IntArrayTarget(), data ).array();
        typeOffsets = StreamVByte.readIntDeltas( new StreamVByte.IntArrayTarget(), data ).array();
    }

    @Override
    public void reset()
    {
        super.reset();
        nodeId = -1;
        expectedType = -1;
        expectedDirection = null;
        typeOffsets = null;
        typesInNode = null;
        currentTypeIndex = -1;
        currentTypeDataIndex = -1;
        loadedCorrectNode = false;
        currentRelationshipOtherNode = -1;
        currentRelationshipHasProperties = false;
        currentRelationshipPropertiesIndex = -1;
        currentTypePropertiesIndex = -1;
        currentTypePropertiesOffset = -1;
        currentRelationshipDirection = null;
    }

    @Override
    public int type()
    {
        return typesInNode[currentTypeIndex];
    }

    @Override
    public long sourceNodeReference()
    {
        return originNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return neighbourNodeReference();
    }

    @Override
    public long neighbourNodeReference()
    {
        return currentRelationshipDirection.equals( OUTGOING ) ? currentRelationshipOtherNode : nodeId;
    }

    @Override
    public long originNodeReference()
    {
        return currentRelationshipDirection.equals( INCOMING ) ? currentRelationshipOtherNode : nodeId;
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
        readHeader();
        readRelationshipTypeGroups();
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
        readHeader();
        expectedType = type;
        expectedDirection = direction;

        // Here we come from a place where the relationship data is already loaded in the FrekiRelationshipGroupCursor so we don't have to
        // read it again... merely initialize the state in here to make it look like it has one group (this type) and let next() enjoy this data.
        // The reference is the data buffer offset, so just go there.
        typesInNode = new int[]{type};
        data.position( (int) reference );
        currentTypeIndex = 0;
    }
}
