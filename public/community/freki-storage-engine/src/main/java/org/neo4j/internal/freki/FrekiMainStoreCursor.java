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

import org.neo4j.io.pagecache.PageCursor;

abstract class FrekiMainStoreCursor implements AutoCloseable
{
    final Store mainStore;
    Record record = new Record( 1 );
    ByteBuffer data;
    private PageCursor cursor;

    // state from record
    int labelsOffset;
    int propertiesOffset;
    int relationshipsOffset;
    int endOffset;

    FrekiMainStoreCursor( Store mainStore )
    {
        this.mainStore = mainStore;
        reset();
    }

    public void reset()
    {
        data = null;
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
        if ( !record.hasFlag( Record.FLAG_IN_USE ) )
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
        if ( otherRecord.hasFlag( Record.FLAG_IN_USE ) )
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
        propertiesOffset = MutableNodeRecordData.propertyOffset( offsetsHeader );
        endOffset = MutableNodeRecordData.endOffset( offsetsHeader );
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
