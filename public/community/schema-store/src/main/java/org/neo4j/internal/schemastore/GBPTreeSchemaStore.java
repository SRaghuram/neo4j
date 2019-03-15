/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.schemastore;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header.TreeCreationChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaRuleMapifier;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.internal.helpers.collection.Iterables.singleOrNull;

public class GBPTreeSchemaStore extends LifecycleAdapter implements Closeable
{
    public static final long MAX_ID = 1L << (6 * Byte.SIZE);

    private final GBPTree<SchemaKey,SchemaValue> tree;
    private final IdGenerator idGenerator;
    private final SchemaLayout layout;
    private final TokenNameLookup tokenNameLookup;

    public GBPTreeSchemaStore( PageCache pageCache, File file, RecoveryCleanupWorkCollector cleanupWorkCollector, IdGeneratorFactory idGeneratorFactory,
            TokenNameLookup tokenNameLookup, boolean readOnly, PageCacheTracer tracer, PageCursorTracer cursorTracer )
    {
        this.tokenNameLookup = tokenNameLookup;
        this.layout = new SchemaLayout();
        TreeCreationChecker creationChecker = new TreeCreationChecker();
        ImmutableSet<OpenOption> openOptions = Sets.immutable.empty();
        this.tree =
                new GBPTree<>( pageCache, file, layout, 0, NO_MONITOR, creationChecker, NO_HEADER_WRITER, cleanupWorkCollector, readOnly, tracer, openOptions );
        File idFile = new File( file.getAbsolutePath() + ".id" );
        LongSupplier highId = highIdScanner( cursorTracer );
        idGenerator = creationChecker.wasCreated()
              ? idGeneratorFactory.create( pageCache, idFile, IdType.SCHEMA, highId.getAsLong(), false, MAX_ID, readOnly, cursorTracer, openOptions )
              : idGeneratorFactory.open( pageCache, idFile, IdType.SCHEMA, highId, MAX_ID, readOnly, cursorTracer, openOptions );
    }

    private LongSupplier highIdScanner( PageCursorTracer cursorTracer )
    {
        return () ->
        {
            MutableLong highestId = new MutableLong();
            visitEntries( ( key, value ) ->
            {
                highestId.setValue( key.id );
                return true; // i.e. abort the scan
            }, false, cursorTracer );
            return highestId.longValue();
        };
    }

    public List<SchemaRule> loadRules( PageCursorTracer cursorTracer ) throws MalformedSchemaRuleException
    {
        return visitEntries( new SchemaRuleLoader(), true, cursorTracer ).loadedRules();
    }

    public SchemaRule loadRule( long id, PageCursorTracer cursorTracer )
            throws MalformedSchemaRuleException, SchemaRuleNotFoundException
    {
        return singleOrNull( visitEntries( new SchemaRuleLoader(), true, cursorTracer, new SchemaKey( id ), new SchemaKey( id + 1 ) ).loadedRules() );
    }

    public <T extends SchemaRule> T loadRule( T ruleDefinition, PageCursorTracer cursorTracer )
            throws MalformedSchemaRuleException, SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        SchemaRule[] rules = loadRules( cursorTracer ).stream().filter( ruleDefinition::equals ).toArray( SchemaRule[]::new );
        if ( rules.length == 0 )
        {
            throw new SchemaRuleNotFoundException( ruleDefinition, tokenNameLookup );
        }
        if ( rules.length > 1 )
        {
            throw new DuplicateSchemaRuleException( ruleDefinition, tokenNameLookup );
        }
        return (T) rules[0];
    }

    private <E extends Exception,V extends EntryVisitor<E>> V visitEntries( V visitor, boolean forwards, PageCursorTracer cursorTracer ) throws E
    {
        SchemaKey low = new SchemaKey( Integer.MIN_VALUE );
        SchemaKey high = new SchemaKey( Integer.MAX_VALUE );
        return visitEntries( visitor, forwards, cursorTracer, low, high );
    }

    private <E extends Exception,V extends EntryVisitor<E>> V visitEntries( V visitor, boolean forwards, PageCursorTracer cursorTracer,
            SchemaKey low, SchemaKey high ) throws E
    {
        try ( Seeker<SchemaKey,SchemaValue> seek = tree.seek( forwards ? low : high, forwards ? high : low, cursorTracer ) )
        {
            while ( seek.next() )
            {
                if ( visitor.accept( seek.key(), seek.value() ) )
                {
                    break;
                }
            }
            return visitor;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public synchronized long nextSchemaRuleId( PageCursorTracer cursorTracer )
    {
        return idGenerator.nextId( cursorTracer );
    }

    public synchronized void writeRule( SchemaRule rule, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Writer<SchemaKey,SchemaValue> writer = tree.writer( cursorTracer ) )
        {
            Map<String,Value> schemaRuleData = SchemaRuleMapifier.mapifySchemaRule( rule );
            for ( Map.Entry<String,Value> entry : schemaRuleData.entrySet() )
            {
                SchemaKey key = new SchemaKey( rule.getId() );
                SchemaValue value = new SchemaValue();
                value.value = entry.getValue();
                key.nameBytes = UTF8.encode( entry.getKey() );
                writer.put( key, value );
            }

            // Bump the highId here just in case, because a crash may leave highId lower than the highest in existence (as found in the tx log)
            try ( IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
            {
                marker.markUsed( rule.getId() );
            }
        }
    }

    /**
     * Deletes a {@link SchemaRule} from this store. This operations is idempotent.
     * @param id the id of the {@link SchemaRule} to delete.
     * @param cursorTracer {@link PageCursorTracer} for tracing page cursor access.
     * @return {@code true} if this rule existed and was removed, otherwise {@code false}.
     * @throws IOException on page cursor I/O error.
     */
    public synchronized boolean deleteRule( long id, PageCursorTracer cursorTracer ) throws IOException
    {
        // Which entries to delete?
        List<SchemaKey> keysToDelete = new ArrayList<>();
        SchemaKey from = new SchemaKey( id );
        from.nameBytes = new byte[0];
        SchemaKey to = new SchemaKey( id + 1 );
        to.nameBytes = new byte[0];
        try ( Seeker<SchemaKey,SchemaValue> seek = tree.seek( from, to, PageCursorTracer.NULL ) )
        {
            while ( seek.next() )
            {
                keysToDelete.add( layout.copyKey( seek.key(), layout.newKey() ) );
            }
        }

        // Delete them
        try ( Writer<SchemaKey,SchemaValue> writer = tree.writer( cursorTracer ) )
        {
            keysToDelete.forEach( writer::remove );
        }
        if ( keysToDelete.isEmpty() )
        {
            // Deleting a rule is idempotent
            return false;
        }
        try ( IdGenerator.Marker marker = idGenerator.marker( cursorTracer ) )
        {
            marker.markDeleted( id );
        }
        return true;
    }

    @Override
    public void start() throws Exception
    {
        idGenerator.start( visitor ->
        {
            MutableLong prevId = new MutableLong();
            visitEntries( ( key, value ) ->
            {
                while ( prevId.longValue() + 1 < key.id )
                {
                    visitor.accept( prevId.incrementAndGet() );
                }
                prevId.setValue( key.id );
                return false;
            }, true, PageCursorTracer.NULL );
            return prevId.longValue();
        }, PageCursorTracer.NULL );
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

    interface EntryVisitor<E extends Exception>
    {
        boolean accept( SchemaKey key, SchemaValue value ) throws E;
    }
}
