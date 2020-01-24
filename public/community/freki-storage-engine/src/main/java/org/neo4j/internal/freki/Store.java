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

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;

public class Store extends BareBoneStore implements SimpleStore
{
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType idType;
    private final int recordsPerPage;
    private final int recordSize;
    private final int sizeExp;

    private IdGenerator idGenerator;

    public Store( FileSystemAbstraction fs, File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, IdType idType, boolean readOnly,
            boolean createIfNotExists, int sizeExp, PageCursorTracerSupplier tracerSupplier )
    {
        super( fs, file, pageCache, readOnly, createIfNotExists, tracerSupplier );
        this.idGeneratorFactory = idGeneratorFactory;
        this.idType = idType;
        this.sizeExp = sizeExp;
        this.recordSize = Record.recordSize( sizeExp );
        this.recordsPerPage = pageCache.pageSize() / recordSize;
    }

    @Override
    public void init() throws IOException
    {
        super.init();
        idGenerator = idGeneratorFactory.open( pageCache, idFileName(), idType, () -> 0, 1L << (6 * Byte.SIZE), readOnly, tracerSupplier.get(),
                openOptions( false ) );
    }

    @Override
    public void shutdown()
    {
        idGenerator.close();
        super.shutdown();
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        return idGenerator.nextId( cursorTracer );
    }

    private File idFileName()
    {
        return new File( file.getAbsolutePath() + ".id" );
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
    public int recordSize()
    {
        return recordSize;
    }

    @Override
    public int recordSizeExponential()
    {
        return sizeExp;
    }

    @Override
    public void write( PageCursor cursor, Record record ) throws IOException
    {
        long id = record.id;
        long pageId = id / recordsPerPage;
        int offset = (int) ((id % recordsPerPage) * Record.SIZE_BASE);
        if ( !cursor.next( pageId ) )
        {
            throw new IllegalStateException( "Could not grow file?" );
        }
        cursor.setOffset( offset );
        record.serialize( cursor );
        cursor.checkAndClearBoundsFlag();
    }

    @Override
    public boolean read( PageCursor cursor, Record record, long id )
    {
        record.clear();
        long pageId = id / recordsPerPage;
        int offset = (int) ((id % recordsPerPage) * Record.SIZE_BASE);
        try
        {
            if ( !cursor.next( pageId ) )
            {
                return false;
            }
            record.id = id;
            do
            {
                cursor.setOffset( offset );
                record.loadRecord( cursor );
            }
            while ( cursor.shouldRetry() );
            cursor.checkAndClearBoundsFlag();
            cursor.checkAndClearCursorException();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return true;
    }

    @Override
    public boolean exists( long id ) throws IOException
    {
//        try ( PageCursor cursor = openReadCursor() )
//        {
//            Record record = new Record( 1 );
//            return read( cursor, record, id ) && record.hasFlag( Record.FLAG_IN_USE );
//        }
        return true;
    }
}
