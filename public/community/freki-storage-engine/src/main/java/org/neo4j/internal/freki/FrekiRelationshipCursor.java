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

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
<<<<<<< HEAD
=======
import org.neo4j.memory.MemoryTracker;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

abstract class FrekiRelationshipCursor extends FrekiMainStoreCursor implements StorageRelationshipCursor
{
    Iterator<StorageProperty> densePropertiesItr;

<<<<<<< HEAD
    FrekiRelationshipCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer );
=======
    FrekiRelationshipCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer,
            MemoryTracker memoryTracker )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    abstract int currentRelationshipPropertiesOffset();

    abstract Iterator<StorageProperty> denseProperties();
}
