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

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.util.EagerDegrees;

import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.nonEmptyIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;

class FrekiNodeCursor extends FrekiMainStoreCursor implements StorageNodeCursor
{
    private long singleId;
    private FrekiRelationshipTraversalCursor relationshipsCursor;

    FrekiNodeCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
    }

    @Override
    public long[] labels()
    {
        ByteBuffer smallRecordData = smallRecord.dataForReading();
        smallRecordData.position( labelsOffset );
        return readIntDeltas( new StreamVByte.LongArrayTarget(), smallRecordData ).array();
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
    public long relationshipsReference()
    {
        return loadedNodeId;
    }

    @Override
    public void relationships( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection )
    {
        traversalCursor.init( this, selection );
    }

    @Override
    public int[] relationshipTypes()
    {
        readRelationshipTypes();
        return relationshipTypesInNode.clone();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        if ( relationshipsCursor == null )
        {
            relationshipsCursor = new FrekiRelationshipTraversalCursor( stores, cursorTracer );
        }
        EagerDegrees degrees = new EagerDegrees();
        relationshipsCursor.init( this, selection );
        while ( relationshipsCursor.next() )
        {
            degrees.add( relationshipsCursor.type(), relationshipsCursor.currentDirection(), 1 );
        }
        return degrees;
    }

    @Override
    public boolean supportsFastDegreeLookup()
    {
        // For simplicity degree lookup involves an internal relationships cursor, but looping through the data is cheap
        // and the data is right there in the buffer
        return true;
    }

    @Override
    public void scan()
    {
//        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void single( long reference )
    {
        singleId = reference;
    }

    @Override
    public boolean hasProperties()
    {
        return nonEmptyIntDeltas( data.array(), nodePropertiesOffset );
    }

    @Override
    public long propertiesReference()
    {
        return entityReference();
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        propertyCursor.initNodeProperties( this );
    }

    @Override
    public long entityReference()
    {
        return loadedNodeId;
    }

    @Override
    public boolean next()
    {
        if ( singleId != NULL )
        {
            loadMainRecord( singleId );
            singleId = NULL;
            return record.hasFlag( FLAG_IN_USE );
        }
        return false;
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = NULL;
        if ( relationshipsCursor != null )
        {
            relationshipsCursor.reset();
        }
    }

    @Override
    public void close()
    {
        if ( relationshipsCursor != null )
        {
            relationshipsCursor.close();
            relationshipsCursor = null;
        }
        super.close();
    }
}
