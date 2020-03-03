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

import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

class FrekiRelationshipScanCursor extends FrekiRelationshipCursor implements StorageRelationshipScanCursor
{
    private long singleId;
    private boolean needsLoading;
    private boolean inScan;
    private long scanNodeId;
    private RelationshipSelection selection;

    private final FrekiRelationshipTraversalCursor traversalCursor;

    FrekiRelationshipScanCursor( MainStores stores, PageCursorTracer cursorTracer )
    {
        super( stores, cursorTracer );
        traversalCursor = new FrekiRelationshipTraversalCursor( stores, cursorTracer );
    }

    @Override
    public boolean next()
    {
        if ( inScan )
        {
            while ( scanNodeId < stores.mainStore.getHighId() )
            {
                if ( needsLoading )
                {
                    needsLoading = false;
                    traversalCursor.init( scanNodeId++, selection ); //load next node
                }

                if ( traversalCursor.next() )
                {
                    return true; // found a relationship
                }
                needsLoading = true; // reached end of relationships of this node
            }
            inScan = false;
            scanNodeId = NULL;
        }
        else if ( singleId != NULL )
        {
            if ( needsLoading )
            {
                needsLoading = false;
                traversalCursor.init( MutableNodeRecordData.nodeIdFromRelationshipId( singleId ), selection );
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
    public void reset()
    {
        super.reset();
        singleId = NULL;
        needsLoading = false;
        inScan = false;
        scanNodeId = NULL;
        selection = null;
    }

    @Override
    public void scan( int type )
    {
        //We scan all nodes, traversing only outgoing relationships to not get duplicates
        inScan = true;
        traversalCursor.reset();
        selection = type == -1
                        ? RelationshipSelection.selection( Direction.OUTGOING )
                        : RelationshipSelection.selection( type, Direction.OUTGOING );
        scanNodeId = 0;
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
        needsLoading = true;
        selection = RelationshipSelection.ALL_RELATIONSHIPS;
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
    public long propertiesReference()
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
    public void close()
    {
        super.close();
        traversalCursor.close();
    }
}
