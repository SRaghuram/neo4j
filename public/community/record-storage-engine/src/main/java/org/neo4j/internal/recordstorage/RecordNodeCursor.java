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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static java.lang.Math.min;

public class RecordNodeCursor extends NodeRecord implements StorageNodeCursor
{
    private final NodeStore read;
    private final PageCursorTracer cursorTracer;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private PageCursor pageCursor;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private boolean open;
    private boolean batched;
    private RecordRelationshipGroupCursor groupCursor;

    RecordNodeCursor( NodeStore read, RelationshipStore relationshipStore, RelationshipGroupStore groupStore, PageCursorTracer cursorTracer )
    {
        super( NO_ID );
        this.read = read;
        this.cursorTracer = cursorTracer;
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
    }

    @Override
    public void scan()
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( 0 );
        }
        this.next = 0;
        this.highMark = nodeHighMark();
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public void single( long reference )
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( reference );
        }
        this.next = reference >= 0 ? reference : NO_ID;
        //This marks the cursor as a "single cursor"
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = NO_ID;

        return ((RecordNodeScan) scan).scanBatch( sizeHint , this);
    }

    boolean scanRange( long start, long stop )
    {
        long max = nodeHighMark();
        if ( start > max )
        {
            reset();
            return false;
        }
        if ( start > stop )
        {
            reset();
            return true;
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( start );
        }
        next = start;
        highMark = min( stop, max );
        return true;
    }

    @Override
    public long entityReference()
    {
        return getId();
    }

    @Override
    public long[] labels()
    {
        return NodeLabelsField.get( this, read, cursorTracer );
    }

    @Override
    public boolean hasLabel( int label )
    {
        //Get labels from store and put in intSet, unfortunately we get longs back
        long[] longs = NodeLabelsField.get( this, read, cursorTracer );
        for ( long labelToken : longs )
        {
            if ( labelToken == label )
            {
                assert (int) labelToken == labelToken : "value too big to be represented as and int";
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != NO_ID;
    }

    @Override
    public long relationshipsReference()
    {
        return getNextRel();
    }

    @Override
    public void relationships( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection )
    {
        traversalCursor.init( entityReference(), getNextRel(), isDense(), selection );
    }

    @Override
    public int[] relationshipTypes()
    {
        MutableIntSet types = IntSets.mutable.empty();
        if ( groupCursor == null )
        {
            groupCursor = new RecordRelationshipGroupCursor( relationshipStore, groupStore, cursorTracer );
        }
        groupCursor.init( entityReference(), getNextRel(), isDense() );
        while ( groupCursor.next() )
        {
            types.add( groupCursor.getType() );
        }
        return types.toArray();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        if ( groupCursor == null )
        {
            groupCursor = new RecordRelationshipGroupCursor( relationshipStore, groupStore, cursorTracer );
        }
        groupCursor.init( entityReference(), getNextRel(), isDense() );
        EagerDegrees result = new EagerDegrees();
        while ( groupCursor.next() )
        {
            if ( selection.test( groupCursor.getType() ) )
            {
                result.add( groupCursor.getType(), groupCursor.outgoingCount(), groupCursor.incomingCount(), groupCursor.totalCount() );
            }
        }
        return result;
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        propertyCursor.initNodeProperties( getNextProp() );
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            resetState();
            return false;
        }

        do
        {
            if ( nextStoreReference == next )
            {
                nodeAdvance( this, pageCursor );
                next++;
                nextStoreReference++;
            }
            else
            {
                node( this, next++, pageCursor );
                nextStoreReference = next;
            }

            if ( next > highMark )
            {
                if ( isSingle() || batched )
                {
                    //we are a "single cursor" or a "batched scan"
                    //we don't want to set a new highMark
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    //we are a "scan cursor"
                    //Check if there is a new high mark
                    highMark = nodeHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return inUse();
                    }
                }
            }
        }
        while ( !inUse() );
        return true;
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            resetState();
        }
    }

    private void resetState()
    {
        next = NO_ID;
        setId( NO_ID );
        clear();
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    @Override
    public RecordNodeCursor copy()
    {
        throw new UnsupportedOperationException( "Record cursors are not copyable." );
    }

    @Override
    public String toString()
    {
        if ( !open )
        {
            return "RecordNodeCursor[closed state]";
        }
        else
        {
            return "RecordNodeCursor[id=" + getId() +
                    ", open state with: highMark=" + highMark +
                    ", next=" + next +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        if ( groupCursor != null )
        {
            groupCursor.close();
            groupCursor = null;
        }
    }

    private PageCursor nodePage( long reference )
    {
        return read.openPageCursorForReading( reference, cursorTracer );
    }

    private long nodeHighMark()
    {
        return read.getHighestPossibleIdInUse();
    }

    private void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        read.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    private void nodeAdvance( NodeRecord record, PageCursor pageCursor )
    {
        read.nextRecordByCursor( record, RecordLoad.CHECK, pageCursor );
    }
}
