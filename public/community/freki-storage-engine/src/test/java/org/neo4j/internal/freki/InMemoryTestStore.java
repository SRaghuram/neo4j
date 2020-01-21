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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;

class InMemoryTestStore implements SimpleStore
{
    private final MutableLongObjectMap<Record> data = LongObjectMaps.mutable.empty();

    @Override
    public PageCursor openWriteCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public void write( PageCursor cursor, Record record )
    {
        Record copy = new Record( 1, 0 );
        copy.copyContentsFrom( record );
        data.put( record.id, copy );
    }

    @Override
    public PageCursor openReadCursor()
    {
        return NO_PAGE_CURSOR;
    }

    @Override
    public boolean read( PageCursor cursor, Record record, long id )
    {
        Record source = data.get( id );
        if ( source == null )
        {
            return false;
        }
        record.copyContentsFrom( source );
        return true;
    }

    // Basically this isn't used, it's just something to call close()
    private static PageCursor NO_PAGE_CURSOR = new ByteArrayPageCursor( new byte[0] );
}
