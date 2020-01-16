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
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;

import static org.neo4j.internal.freki.MutableNodeRecordData.SIZE_SLOT_HEADER;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;

class FrekiNodeCursor implements StorageNodeCursor
{
    private final Store mainStore;
    Record record = new Record( 1 );
    private ByteBuffer data;

    private PageCursor cursor;
    private long singleId;
    private long scanId;

    FrekiNodeCursor( Store mainStore )
    {
        this.mainStore = mainStore;
        reset();
    }

    @Override
    public long[] labels()
    {
        StreamVByte.LongArrayTarget target = new StreamVByte.LongArrayTarget();
        // TODO not position(), more like right after the record header, right?
        readIntDeltas( target, data );
        return target.array();
    }

    @Override
    public boolean hasLabel( int label )
    {
        for ( long labelId : labels() )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long relationshipGroupReference()
    {
        return 0;
    }

    @Override
    public long allRelationshipsReference()
    {
        return 0;
    }

    @Override
    public boolean isDense()
    {
        return false;
    }

    @Override
    public void scan()
    {
        scanId = 0;
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
        return false;
    }

    @Override
    public void single( long reference )
    {
        singleId = reference;
    }

    @Override
    public boolean hasProperties()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long propertiesReference()
    {
        return record.id;
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long entityReference()
    {
        return record.id;
    }

    @Override
    public boolean next()
    {
        if ( singleId != -1 )
        {
            mainStore.read( cursor(), record, singleId );
            singleId = -1;
            if ( !record.hasFlag( Record.FLAG_IN_USE ) )
            {
                return false;
            }
            data = record.dataForReading();
            // Skip the offset header
            data.position( data.position() + SIZE_SLOT_HEADER );
            return record.hasFlag( Record.FLAG_IN_USE );
        }
        return false;
    }

    private PageCursor cursor()
    {
        if ( cursor == null )
        {
            cursor = mainStore.openReadCursor();
        }
        return cursor;
    }

    @Override
    public void reset()
    {
        singleId = -1;
        scanId = -1;
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
