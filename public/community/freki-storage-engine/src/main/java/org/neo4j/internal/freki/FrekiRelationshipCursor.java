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

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.util.Preconditions.checkState;

abstract class FrekiRelationshipCursor extends FrekiMainStoreCursor implements StorageRelationshipCursor
{
    int[] typesInNode;
    int[] typeOffsets;
    int firstTypeOffset;

    FrekiRelationshipCursor( Store mainStore )
    {
        super( mainStore );
    }

    void readRelationshipTypeGroups()
    {
        data.position( relationshipsOffset );
        typesInNode = readIntDeltas( new IntArrayTarget(), data ).array();
        // Right after the types array the relationship group data starts, so this is the offset for the first type
        firstTypeOffset = data.position();

        // Then read the rest of the offsets for type indexes > 0 after all the relationship data groups, i.e. at endOffset
        // The values in the typeOffsets array are relative to the firstTypeOffset
        IntArrayTarget typeOffsetsTarget = new IntArrayTarget();
        readIntDeltas( typeOffsetsTarget, data.array(), endOffset );
        typeOffsets = typeOffsetsTarget.array();
    }

    int relationshipPropertiesOffset( int relationshipGroupPropertiesOffset, int relationshipIndexInGroup )
    {
        if ( relationshipIndexInGroup == -1 )
        {
            return -1;
        }

        checkState( relationshipGroupPropertiesOffset >= 0, "Should not be called if this relationship has no properties" );
        int offset = relationshipGroupPropertiesOffset;
        for ( int i = 0; i < relationshipIndexInGroup; i++ )
        {
            int blockSize = data.get( offset );
            offset += blockSize;
        }
        return offset + 1;
    }

    int relationshipTypeOffset( int typeIndex )
    {
        return typeIndex == 0 ? firstTypeOffset : firstTypeOffset + typeOffsets[typeIndex - 1];
    }

    @Override
    public void reset()
    {
        super.reset();
        typeOffsets = null;
        typesInNode = null;
    }

    abstract int currentRelationshipPropertiesOffset();
}
