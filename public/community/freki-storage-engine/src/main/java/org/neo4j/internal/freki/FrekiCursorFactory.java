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

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllRelationshipsScan;

class FrekiCursorFactory // almost implements CursorFactory
{
    private final MainStores stores;
    private final CursorAccessPatternTracer cursorAccessPatternTracer;

    FrekiCursorFactory( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer )
    {
        this.stores = stores;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
    }

    public AllRelationshipsScan allRelationshipScan()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    public FrekiNodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiNodeCursor( stores, cursorAccessPatternTracer, cursorTracer, null ); //Awaiting next MT propagation PR.
    }

    public FrekiPropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return new FrekiPropertyCursor( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker ); //Awaiting next MT propagation PR.
    }

    public FrekiRelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiRelationshipTraversalCursor( stores, cursorAccessPatternTracer, cursorTracer, null ); //Awaiting next MT propagation PR.
    }

    public FrekiRelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiRelationshipScanCursor( stores, cursorAccessPatternTracer, cursorTracer, null ); //Awaiting next MT propagation PR.
    }
}
