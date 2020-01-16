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
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.internal.helpers.ArrayUtil.concat;

public class Store extends LifecycleAdapter implements AutoCloseable
{
    private final FileSystemAbstraction fs;
    private final File file;
    private final PageCache pageCache;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType idType;
    private final boolean readOnly;
    private final boolean createIfNotExists;
    private final PageCursorTracerSupplier tracerSupplier;
    private final int recordsPerPage;

    private PagedFile mappedFile;
    private IdGenerator idGenerator;

    public Store( FileSystemAbstraction fs, File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, IdType idType, boolean readOnly,
            boolean createIfNotExists, PageCursorTracerSupplier tracerSupplier )
    {
        this.fs = fs;
        this.file = file;
        this.pageCache = pageCache;
        this.idGeneratorFactory = idGeneratorFactory;
        this.idType = idType;
        this.readOnly = readOnly;
        this.createIfNotExists = createIfNotExists;
        this.tracerSupplier = tracerSupplier;
        this.recordsPerPage = pageCache.pageSize() / Record.SIZE_BASE;
    }

    @Override
    public void init() throws IOException
    {
        OpenOption[] openOptions = openOptions();
        mappedFile = pageCache.map( file, pageCache.pageSize(), openOptions );
        idGenerator = idGeneratorFactory.open( pageCache, idFileName(), idType, () -> 0, 1L << (6 * Byte.SIZE), readOnly, tracerSupplier.get(), openOptions );
    }

    @Override
    public void shutdown()
    {
        idGenerator.close();
        mappedFile.close();
    }

    private OpenOption[] openOptions()
    {
        OpenOption[] openOptions = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        openOptions = createIfNotExists ? concat( openOptions, StandardOpenOption.CREATE ) : openOptions;
        return openOptions;
    }

    private File idFileName()
    {
        return new File( file.getAbsolutePath() + ".id" );
    }

    public void flush( PageCursorTracer cursorTracer )
    {
    }

    @Override
    public void close()
    {
        shutdown();
    }

    PageCursor openWriteCursor() throws IOException
    {
        return mappedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, PageCursorTracer.NULL );
    }

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
    }

    PageCursor openReadCursor()
    {
        try
        {
            return mappedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK, PageCursorTracer.NULL );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

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
                record.deserialize( cursor );
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

    boolean exists( long id ) throws IOException
    {
//        try ( PageCursor cursor = openReadCursor() )
//        {
//            Record record = new Record( 1 );
//            return read( cursor, record, id ) && record.hasFlag( Record.FLAG_IN_USE );
//        }
        return true;
    }
}
