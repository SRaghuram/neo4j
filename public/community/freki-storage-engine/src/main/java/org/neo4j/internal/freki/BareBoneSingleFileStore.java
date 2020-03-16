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

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.internal.helpers.ArrayUtil.concat;

abstract class BareBoneSingleFileStore extends LifecycleAdapter implements SingleFileStore
{
    final File file;
    final PageCache pageCache;
    final boolean readOnly;
    final int pageSize;
    private final boolean createIfNotExists;
    final PageCursorTracerSupplier tracerSupplier;

    protected PagedFile mappedFile;

    BareBoneSingleFileStore( File file, PageCache pageCache, boolean readOnly, boolean createIfNotExists, PageCursorTracerSupplier tracerSupplier )
    {
        this.file = file;
        this.pageCache = pageCache;
        this.pageSize = pageCache.pageSize();
        this.readOnly = readOnly;
        this.createIfNotExists = createIfNotExists;
        this.tracerSupplier = tracerSupplier;
    }

    @Override
    public void init() throws IOException
    {
        mappedFile = pageCache.map( file, pageCache.pageSize(), openOptions( true ) );
    }

    ImmutableSet<OpenOption> openOptions( boolean considerCreate )
    {
        OpenOption[] openOptions = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        openOptions = considerCreate && createIfNotExists ? concat( openOptions, StandardOpenOption.CREATE ) : openOptions;
        return Sets.immutable.of( openOptions );
    }

    @Override
    public PageCursor openWriteCursor() throws IOException
    {
        return mappedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracerSupplier.get() );
    }

    @Override
    public PageCursor openReadCursor()
    {
        try
        {
            return mappedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK, tracerSupplier.get() );
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
    }

    @Override
    public void shutdown()
    {
        mappedFile.close();
    }
}
