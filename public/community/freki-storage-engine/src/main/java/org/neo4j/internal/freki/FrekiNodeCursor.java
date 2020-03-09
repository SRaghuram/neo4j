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
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.util.EagerDegrees;

import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.isDenseFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.nonEmptyIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;

public class FrekiNodeCursor extends FrekiMainStoreCursor implements StorageNodeCursor
{
    private long singleId;
    private boolean inScan;
    private FrekiRelationshipTraversalCursor relationshipsCursor;

    public FrekiNodeCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
    }

    @Override
    public long[] labels()
    {
        ByteBuffer smallRecordData = smallRecord.dataForReading( headerState.labelsOffset );
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
        // Dense
        if ( headerState.isDense )
        {
            return stores.denseStore.getDegrees( loadedNodeId, RelationshipSelection.ALL_RELATIONSHIPS, cursorTracer ).types();
        }

        // Sparse
        readRelationshipTypesAndOffsets();
        return relationshipTypesInNode.clone();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        // Dense
        if ( headerState.isDense )
        {
            return stores.denseStore.getDegrees( loadedNodeId, selection, cursorTracer );
        }

        // Sparse
        if ( relationshipsCursor == null )
        {
            relationshipsCursor = new FrekiRelationshipTraversalCursor( stores, cursorTracer );
        }
        EagerDegrees degrees = new EagerDegrees();
        relationshipsCursor.init( this, selection );
        // TODO If the selection is for any direction then this can be made more efficient by simply looking at the vbyte relationship array length
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
        // and the data is right there in the buffer. Also for dense nodes the degree lookup is a single lookup in the b+tree
        return true;
    }

    @Override
    public void scan()
    {
        inScan = true;
        singleId = 0;
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
        return nonEmptyIntDeltas( data.array(), headerState.nodePropertiesOffset );
    }

    @Override
    public Reference propertiesReference()
    {
        return FrekiReference.nodeReference( entityReference() );
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
        if ( inScan )
        {
            while ( singleId < stores.mainStore.getHighId() )
            {
                if ( loadMainRecord( singleId++ ) )
                {
                    return true;
                }
            }
            inScan = false;
            singleId = NULL;
        }
        else if ( singleId != NULL )
        {
            loadMainRecord( singleId );
            singleId = NULL;
            return smallRecord.hasFlag( FLAG_IN_USE );
        }
        return false;
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = NULL;
        inScan = false;
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

    @Override
    public String toString()
    {
        long fw = headerState.forwardPointer;
        return String.format( "{nodeId:%d,fw:%s,pOff:%d,rOff:%d}", loadedNodeId, headerState.containsForwardPointer
                ? isDenseFromForwardPointer( fw ) ? "DENSE" : idFromForwardPointer( fw ) + "(" + sizeExponentialFromForwardPointer( fw ) + ")" : "-",
                headerState.nodePropertiesOffset, headerState.relationshipsOffset );
    }
}
