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

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.util.Preconditions.checkState;

abstract class FrekiMainStoreCursor implements AutoCloseable
{
    static final long NULL = -1;
    static final int NULL_OFFSET = 0;

    final SimpleStore mainStore;
    Record record = new Record( 1 );
    ByteBuffer data;
    private PageCursor cursor;

    // state from record data header
    int labelsOffset;
    int nodePropertiesOffset;
    int relationshipsOffset;
    int endOffset;

    // state from relationship section, it's here because both relationship cursors as well as property cursor makes use of them
    int[] relationshipTypesInNode;
    int[] relationshipTypeOffsets;
    int firstRelationshipTypeOffset;

    FrekiMainStoreCursor( SimpleStore mainStore )
    {
        this.mainStore = mainStore;
        reset();
    }

    public void reset()
    {
        data = null;
        relationshipTypeOffsets = null;
        relationshipTypesInNode = null;
    }

    PageCursor cursor()
    {
        if ( cursor == null )
        {
            cursor = mainStore.openReadCursor();
        }
        return cursor;
    }

    boolean loadMainRecord( long id )
    {
        mainStore.read( cursor(), record, id );
        if ( !record.hasFlag( FLAG_IN_USE ) )
        {
            return false;
        }
        data = record.dataForReading();
        readOffsets();
        return true;
    }

    boolean useSharedRecordFrom( FrekiMainStoreCursor alreadyLoadedRecord )
    {
        Record otherRecord = alreadyLoadedRecord.record;
        if ( otherRecord.hasFlag( FLAG_IN_USE ) )
        {
            record.initializeFromWithSharedData( otherRecord );
            data = record.dataForReading();
            readOffsets();
            return true;
        }
        return false;
    }

    private void readOffsets()
    {
        int offsetsHeader = MutableNodeRecordData.readOffsetsHeader( data );
        labelsOffset = data.position();
        relationshipsOffset = MutableNodeRecordData.relationshipOffset( offsetsHeader );
        nodePropertiesOffset = MutableNodeRecordData.propertyOffset( offsetsHeader );
        endOffset = MutableNodeRecordData.endOffset( offsetsHeader );
    }

    void readRelationshipTypes()
    {
        data.position( relationshipsOffset );
        relationshipTypesInNode = readIntDeltas( new IntArrayTarget(), data ).array();
        // Right after the types array the relationship group data starts, so this is the offset for the first type
        firstRelationshipTypeOffset = data.position();

        // Then read the rest of the offsets for type indexes > 0 after all the relationship data groups, i.e. at endOffset
        // The values in the typeOffsets array are relative to the firstTypeOffset
        IntArrayTarget typeOffsetsTarget = new IntArrayTarget();
        readIntDeltas( typeOffsetsTarget, data.array(), endOffset );
        relationshipTypeOffsets = typeOffsetsTarget.array();
    }

    int relationshipPropertiesOffset( int relationshipGroupPropertiesOffset, int relationshipIndexInGroup )
    {
        if ( relationshipIndexInGroup == -1 )
        {
            return NULL_OFFSET;
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
        return typeIndex == 0 ? firstRelationshipTypeOffset : firstRelationshipTypeOffset + relationshipTypeOffsets[typeIndex - 1];
    }

    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }
    }
}
