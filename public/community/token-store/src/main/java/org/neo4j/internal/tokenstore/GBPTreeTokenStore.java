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
package org.neo4j.internal.tokenstore;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header.TreeCreationChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.string.UTF8;
import org.neo4j.token.api.NamedToken;

import static java.lang.Math.toIntExact;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;

public class GBPTreeTokenStore implements Closeable
{
    private final GBPTree<TokenKey,Void> tree;
    private final IdGenerator idGenerator;

    public GBPTreeTokenStore( PageCache pageCache, File file, RecoveryCleanupWorkCollector cleanupWorkCollector, IdGeneratorFactory idGeneratorFactory,
            IdType idType, int maxId, boolean readOnly, PageCacheTracer tracer, PageCursorTracer cursorTracer )
    {
        TreeCreationChecker creationChecker = new TreeCreationChecker();
        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        this.tree = new GBPTree<>( pageCache, file, new TokenLayout(), 0, NO_MONITOR, creationChecker, NO_HEADER_WRITER, cleanupWorkCollector, readOnly,
                tracer, openOptions );
        File idFile = new File( file.getAbsolutePath() + ".id" );
        LongSupplier highId = highIdScanner( cursorTracer );
        idGenerator = creationChecker.wasCreated()
                      ? idGeneratorFactory.create( pageCache, idFile, idType, highId.getAsLong(), false, maxId, readOnly, cursorTracer, openOptions )
                      : idGeneratorFactory.open( pageCache, idFile, idType, highId, maxId, readOnly, cursorTracer, openOptions );
    }

    private LongSupplier highIdScanner( PageCursorTracer cursorTracer )
    {
        return () ->
        {
            try ( Seeker<TokenKey,Void> seek = tree.seek( new TokenKey( Integer.MAX_VALUE ), new TokenKey( Integer.MIN_VALUE ), cursorTracer ) )
            {
                return seek.next() ? seek.key().id : 0;
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    public List<NamedToken> loadTokens( PageCursorTracer cursorTracer ) throws IOException
    {
        List<NamedToken> tokens = new ArrayList<>();
        try ( Seeker<TokenKey,Void> seek = tree.seek( new TokenKey( Integer.MIN_VALUE ), new TokenKey( Integer.MAX_VALUE ), cursorTracer ) )
        {
            while ( seek.next() )
            {
                TokenKey key = seek.key();
                int id = key.id;
                String name = UTF8.decode( key.nameBytes );
                tokens.add( new NamedToken( name, id, false ) );
            }
        }
        return tokens;
    }

    public int nextTokenId( PageCursorTracer cursorTracer )
    {
        return toIntExact( idGenerator.nextId( cursorTracer ) );
    }

    public void setHighId( int newHighId )
    {
        idGenerator.setHighId( newHighId );
    }

    public synchronized void writeToken( NamedToken token, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Writer<TokenKey,Void> writer = tree.writer( cursorTracer ) )
        {
            TokenKey key = new TokenKey( token.id() );
            key.nameBytes = UTF8.encode( token.name() );
            writer.put( key, null );
        }
        try ( IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
        {
            marker.markUsed( token.id() );
        }
    }

    public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
        tree.checkpoint( ioLimiter, cursorTracer );
        idGenerator.checkpoint( ioLimiter, cursorTracer );
    }

    public void close() throws IOException
    {
        IOUtils.closeAll( tree, idGenerator );
    }
}
