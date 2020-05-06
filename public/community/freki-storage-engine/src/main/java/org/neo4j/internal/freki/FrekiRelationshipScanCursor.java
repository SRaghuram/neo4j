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

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.internal.freki.MutableNodeData.nodeIdFromRelationshipId;

class FrekiRelationshipScanCursor extends FrekiRelationshipCursor implements StorageRelationshipScanCursor
{
    private long singleId;
    private long singleTargetNodeReference;
    private boolean needsLoading;
    private boolean inScan;
    private RelationshipSelection selection;

    private final FrekiRelationshipTraversalCursor traversalCursor;
    private final FrekiNodeCursor nodeCursor;

    FrekiRelationshipScanCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer,
            MemoryTracker memoryTracker )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
        traversalCursor = new FrekiRelationshipTraversalCursor( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
        nodeCursor = new FrekiNodeCursor( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
    }

    @Override
    public boolean next()
    {
        if ( inScan )
        {
            while ( traversalCursor.next() || (needsLoading = nodeCursor.next()) )
            {
                if ( needsLoading )
                {
                    traversalCursor.init( nodeCursor, selection ); //load next node
                    needsLoading = false;
                }
                else
                {
                    return true;
                }
            }
            inScan = false;
        }
        else if ( singleId != NULL )
        {
            if ( needsLoading )
            {
                needsLoading = false;
                cursorAccessTracer.registerRelationshipByReference( singleId );
                traversalCursor.initInternal( nodeIdFromRelationshipId( singleId ), singleTargetNodeReference, selection );
            }
            while ( traversalCursor.next() )
            {
                if ( traversalCursor.entityReference() == singleId )
                {
                    return true;
                }
            }
            singleId = NULL;
        }
        return false;
    }

    @Override
    int currentRelationshipPropertiesOffset()
    {
        return traversalCursor.currentRelationshipPropertiesOffset();
    }

    @Override
    Iterator<StorageProperty> denseProperties()
    {
        return traversalCursor.denseProperties();
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = NULL;
        singleTargetNodeReference = NULL;
        needsLoading = false;
        inScan = false;
        selection = null;
    }

    @Override
    public void scan( int type )
    {
        //We scan all nodes, traversing only outgoing relationships to not get duplicates
        inScan = true;
        traversalCursor.reset();
        nodeCursor.reset();
        nodeCursor.scan( false );
        selection = type == -1
                        ? RelationshipSelection.selection( Direction.OUTGOING )
                        : RelationshipSelection.selection( type, Direction.OUTGOING );
        needsLoading = true;
    }

    @Override
    public void scan()
    {
        scan( -1 );
    }

    @Override
    public boolean scanBatch( AllRelationshipsScan scan, int sizeHint )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void single( long reference )
    {
        traversalCursor.reset();
        singleId = reference;
        singleTargetNodeReference = NULL;
        needsLoading = true;
        selection = RelationshipSelection.ALL_RELATIONSHIPS;
    }

    @Override
    public void single( long reference, long sourceNodeReference, int type, long targetNodeReference )
    {
        traversalCursor.reset();
        singleId = reference;
        singleTargetNodeReference = targetNodeReference;
        needsLoading = true;
        selection = RelationshipSelection.selection( type, Direction.OUTGOING );
    }

    @Override
    public int type()
    {
        return traversalCursor.type();
    }

    @Override
    public long sourceNodeReference()
    {
        return traversalCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return traversalCursor.targetNodeReference();
    }

    @Override
    public boolean hasProperties()
    {
        return traversalCursor.hasProperties();
    }

    @Override
    public Reference propertiesReference()
    {
        return traversalCursor.propertiesReference();
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        traversalCursor.properties( propertyCursor );
    }

    @Override
    public long entityReference()
    {
        return traversalCursor.entityReference();
    }

    @Override
    public Reference relationshipReference()
    {
        return traversalCursor.relationshipReference();
    }

    @Override
    boolean initializeOtherCursorFromStateOfThisCursor( FrekiMainStoreCursor otherCursor )
    {
        // This scan cursor is an empty shell, just forward all things to the inner traversal cursor
        return traversalCursor.initializeOtherCursorFromStateOfThisCursor( otherCursor );
    }

    @Override
    public void close()
    {
        super.close();
        traversalCursor.close();
    }
}
