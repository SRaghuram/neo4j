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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.util.IdUpdateListener;

import static java.lang.String.format;
import static org.neo4j.internal.freki.Record.recordSize;
import static org.neo4j.internal.freki.Record.recordXFactor;

public class Store extends BareBoneSingleFileStore implements SimpleStore
{
    private final int sizeExp;

    public Store( File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, IdType idType, boolean readOnly, boolean createIfNotExists,
            int sizeExp, PageCacheTracer pageCacheTracer )
    {
        super( file, pageCache, idGeneratorFactory, idType, readOnly, createIfNotExists, recordSize( sizeExp ), pageCacheTracer );
        this.sizeExp = sizeExp;
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        return idGenerator.nextId( cursorTracer );
    }

    @Override
    public Record newRecord()
    {
        return new Record( sizeExp );
    }

    @Override
    public Record newRecord( long id )
    {
        return new Record( sizeExp, id );
    }

    @Override
    public int recordSizeExponential()
    {
        return sizeExp;
    }

    @Override
    public void write( PageCursor cursor, Record record, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer ) throws IOException
    {
        long id = record.id;
        goToId( cursor, id );
        record.serialize( cursor );
        cursor.checkAndClearBoundsFlag();
        idUpdateListener.markId( idGenerator, id, record.hasFlag( Record.FLAG_IN_USE ), cursorTracer );
    }

    private void goToId( PageCursor cursor, long id ) throws IOException
    {
        long pageId = idPage( id );
        int offset = idOffset( id );
        if ( !cursor.next( pageId ) )
        {
            throw new IllegalStateException( "Could not grow file?" );
        }
        cursor.setOffset( offset );
    }

    @Override
    public boolean read( PageCursor cursor, Record record, long id )
    {
        record.clear();
        long pageId = idPage( id );
        int offset = idOffset( id );
        try
        {
            if ( !cursor.next( pageId ) )
            {
                return false;
            }
            record.id = id;
            record.loadRecord( cursor, offset );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return true;
    }

    @Override
    public String toString()
    {
        return format( "Store[x%d,highId:%d,%s]", recordXFactor( sizeExp ), getHighId(), file.getName() );
    }
}
