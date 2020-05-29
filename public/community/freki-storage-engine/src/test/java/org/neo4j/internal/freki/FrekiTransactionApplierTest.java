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

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.AsyncApply;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.CursorAccessPatternTracer.NO_TRACING;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

@EphemeralPageCacheExtension
class FrekiTransactionApplierTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;

    private LifeSupport life = new LifeSupport();
    private Stores stores;
    private RecordingIdGeneratorWorkSync idUpdates;

    @BeforeEach
    void setUp() throws IOException
    {
        stores = life.add( new Stores( fs, DatabaseLayout.ofFlat( directory.directory( "store" ) ), pageCache, new DefaultIdGeneratorFactory( fs, immediate() ),
                PageCacheTracer.NULL, immediate(), true, new StandardConstraintRuleAccessor(), index -> index, EmptyMemoryTracker.INSTANCE ) );
        life.start();
        idUpdates = new RecordingIdGeneratorWorkSync();
        stores.idGenerators( idUpdates::add );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    @Test
    void shouldUpdateIdGeneratorOnCreatedX1Record() throws Exception
    {
        // given
        int sizeExp = 0;
        long nodeId = 0;

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, true ) ).isTrue();
    }

    @Test
    void shouldUpdateIdGeneratorOnCreatedXLRecord() throws Exception
    {
        // given
        int sizeExp = 2;
        long nodeId = 0;

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, true ) ).isTrue();
    }

    @Test
    void shouldUpdateIdGeneratorOonDeletedX1Record() throws Exception
    {
        // given
        int sizeExp = 0;
        long nodeId = 0;
        preState( applier -> apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, usedRecord( sizeExp, nodeId ), null );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, false ) ).isTrue();
    }

    @Test
    void shouldUpdateIdGeneratorOonDeletedXLRecord() throws Exception
    {
        // given
        int sizeExp = 1;
        long nodeId = 0;
        preState( applier -> apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, usedRecord( sizeExp, nodeId ), null );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, false ) ).isTrue();
    }

    @Test
    void shouldNotUpdateIdGeneratorOnUpdatedX1Record() throws Exception
    {
        // given
        int sizeExp = 1;
        long nodeId = 0;
        preState( applier -> apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, usedRecord( sizeExp, nodeId ), usedRecord( sizeExp, nodeId ) );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, true ) ).isFalse();
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, false ) ).isFalse();
    }

    @Test
    void shouldNotUpdateIdGeneratorOnUpdatedXLRecord() throws Exception
    {
        // given
        int sizeExp = 3;
        long nodeId = 0;
        preState( applier -> apply( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            apply( applier, nodeId, usedRecord( sizeExp, nodeId ), usedRecord( sizeExp, nodeId ) );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, true ) ).isFalse();
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, false ) ).isFalse();
    }

    private void apply( FrekiTransactionApplier applier, long nodeId, Record before, Record after ) throws IOException
    {
        FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId );
        command.addChange( before, after );
        applier.handle( command );
    }

    private void preState( ThrowingConsumer<FrekiTransactionApplier,IOException> state ) throws Exception
    {
        try ( FrekiTransactionApplier applier = applier() )
        {
            state.accept( applier );
        }
        idUpdates.clearEvents();
    }

    private Record usedRecord( int sizeExp, long id )
    {
        Record record = new Record( sizeExp, id );
        record.setFlag( Record.FLAG_IN_USE, true );
        return record;
    }

    private FrekiTransactionApplier applier()
    {
        return new FrekiTransactionApplier( stores, new FrekiStorageReader( stores, NO_TRACING, null /*should not be required*/ ), null, null, EXTERNAL,
                idUpdates, null, null, null, PageCacheTracer.NULL, PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE );
    }

    private static class RecordingIdGeneratorWorkSync extends IdGeneratorUpdatesWorkSync
    {
        private final List<Triple<IndexedIdGenerator,Long,Boolean>> events = new ArrayList<>();

        @Override
        public Batch newBatch( PageCacheTracer pageCacheTracer )
        {
            Batch actual = super.newBatch( pageCacheTracer );
            return new Batch( pageCacheTracer )
            {
                @Override
                public void markIdAsUsed( IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
                {
                    actual.markIdAsUsed( idGenerator, id, cursorTracer );
                    events.add( Triple.of( (IndexedIdGenerator) idGenerator, id, true ) );
                }

                @Override
                public void markIdAsUnused( IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
                {
                    actual.markIdAsUnused( idGenerator, id, cursorTracer );
                    events.add( Triple.of( (IndexedIdGenerator) idGenerator, id, false ) );
                }

                @Override
                public AsyncApply applyAsync( PageCacheTracer cacheTracer )
                {
                    return actual.applyAsync( cacheTracer );
                }

                @Override
                public void apply( PageCacheTracer cacheTracer ) throws ExecutionException
                {
                    actual.apply( cacheTracer );
                }

                @Override
                public void close() throws Exception
                {
                    actual.close();
                }
            };
        }

        void clearEvents()
        {
            events.clear();
        }

        boolean hasEvent( int sizeExp, long id, boolean used )
        {
            String xStoreName = format( "x%d", Record.recordXFactor( sizeExp ) );
            return events.stream().anyMatch( e -> e.getLeft().file().getName().contains( xStoreName ) && e.getMiddle() == id && e.getRight() == used );
        }
    }
}