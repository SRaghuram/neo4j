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

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.internal.helpers.ArrayUtil.concat;

public abstract class BareBoneSingleFileStore extends LifecycleAdapter implements SingleFileStore
{
    final File file;
    private final PageCache pageCache;
    private final IdGeneratorFactory idGeneratorFactory;
    final IdType idType;
    final boolean readOnly;
    final int pageSize;
    private final boolean createIfNotExists;
    int recordSize;
    private PageCacheTracer pageCacheTracer;
    final int recordsPerPage;

    protected PagedFile mappedFile;
    protected IdGenerator idGenerator;

    BareBoneSingleFileStore( File file, PageCache pageCache, IdGeneratorFactory idGeneratorFactory, IdType idType, boolean readOnly, boolean createIfNotExists,
            int recordSize, PageCacheTracer pageCacheTracer )
    {
        this.file = file;
        this.pageCache = pageCache;
        this.pageSize = pageCache.pageSize();
        this.idGeneratorFactory = idGeneratorFactory;
        this.idType = idType;
        this.readOnly = readOnly;
        this.createIfNotExists = createIfNotExists;
        this.recordSize = recordSize;
        this.pageCacheTracer = pageCacheTracer;
        this.recordsPerPage = pageCache.pageSize() / recordSize;
    }

    @Override
    public void init() throws IOException
    {
        mappedFile = pageCache.map( file, pageCache.pageSize(), openOptions( true ) );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "Open ID generator" ) )
        {
            idGenerator =
                    idGeneratorFactory.open( pageCache, idFileName(), idType, () -> 0, 1L << (6 * Byte.SIZE), readOnly, cursorTracer, openOptions( false ) );
        }
    }

    @Override
    public void start() throws Exception
    {
        super.start();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "Start ID generator" ) )
        {
            idGenerator.start( freeIdsForRebuild( cursorTracer ), cursorTracer );
        }
    }

    private FreeIds freeIdsForRebuild( PageCursorTracer cursorTracer )
    {
        return visitor ->
        {
            long highestIdFound = -1;
            long[] foundIds = new long[recordsPerPage];
            int foundIdsCursor;
            try ( PageCursor cursor = openReadCursor( cursorTracer ) )
            {
                while ( cursor.next() ) // <-- will stop after last page, since this is a read cursor
                {
                    do
                    {
                        foundIdsCursor = 0;
                        long idPageOffset = cursor.getCurrentPageId() * recordsPerPage;
                        for ( int i = 0; i < recordsPerPage; i++ )
                        {
                            int offset = i * recordSize;
                            cursor.setOffset( offset );
                            if ( !Record.isInUse( cursor, offset ) )
                            {
                                foundIds[foundIdsCursor++] = idPageOffset + i;
                            }
                        }
                    }
                    while ( cursor.shouldRetry() );
                    if ( cursor.checkAndClearBoundsFlag() )
                    {
                        throw new UnderlyingStorageException(
                                "Out of bounds access on page " + cursor.getCurrentPageId() + " detected while scanning the " + file +
                                        " file for deleted records" );
                    }

                    for ( int i = 0; i < foundIdsCursor; i++ )
                    {
                        visitor.accept( foundIds[i] );
                    }
                    if ( foundIdsCursor > 0 )
                    {
                        highestIdFound = foundIds[foundIdsCursor - 1];
                    }
                }
                return highestIdFound;
            }
        };
    }

    private File idFileName()
    {
        return new File( file.getAbsolutePath() + ".id" );
    }

    ImmutableSet<OpenOption> openOptions( boolean considerCreate )
    {
        OpenOption[] openOptions = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        openOptions = considerCreate && createIfNotExists ? concat( openOptions, StandardOpenOption.CREATE ) : openOptions;
        return Sets.immutable.of( openOptions );
    }

    int idOffset( long id )
    {
        return (int) ((id % recordsPerPage) * recordSize);
    }

    long idPage( long id )
    {
        return id / recordsPerPage;
    }

    @Override
    public PageCursor openWriteCursor( PageCursorTracer cursorTracer ) throws IOException
    {
        return mappedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer );
    }

    @Override
    public PageCursor openReadCursor( PageCursorTracer cursorTracer )
    {
        try
        {
            return mappedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void flush( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException
    {
        mappedFile.flushAndForce( ioLimiter );
        idGenerator.checkpoint( ioLimiter, cursorTracer );
    }

    @Override
    public boolean exists( PageCursor cursor, long id )
    {
        long pageId = idPage( id );
        int offset = idOffset( id );
        try
        {
            if ( !cursor.next( pageId ) )
            {
                return false;
            }
            return Record.isInUse( cursor, offset );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void shutdown()
    {
        idGenerator.close();
        mappedFile.close();
    }

    @Override
    public long getHighId()
    {
        return idGenerator.getHighId();
    }
}
