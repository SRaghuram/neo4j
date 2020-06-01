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
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterator;
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
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.CursorAccessPatternTracer.NO_TRACING;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

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
    private DenseRelationshipsWorkSync denseUpdates;

    @BeforeEach
    void setUp() throws IOException
    {
        stores = life.add( new Stores( fs, DatabaseLayout.ofFlat( directory.directory( "store" ) ), pageCache, new DefaultIdGeneratorFactory( fs, immediate() ),
                PageCacheTracer.NULL, immediate(), true, new StandardConstraintRuleAccessor(), index -> index, EmptyMemoryTracker.INSTANCE ) );
        life.start();
        idUpdates = new RecordingIdGeneratorWorkSync();
        denseUpdates = new DenseRelationshipsWorkSync( stores.denseStore );
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
            applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) );
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
            applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) );
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
        preState( applier -> applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            applySparseCommand( applier, nodeId, usedRecord( sizeExp, nodeId ), null );
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
        preState( applier -> applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            applySparseCommand( applier, nodeId, usedRecord( sizeExp, nodeId ), null );
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
        preState( applier -> applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            applySparseCommand( applier, nodeId, usedRecord( sizeExp, nodeId ), usedRecord( sizeExp, nodeId ) );
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
        preState( applier -> applySparseCommand( applier, nodeId, null, usedRecord( sizeExp, nodeId ) ) );

        // when
        try ( FrekiTransactionApplier applier = applier() )
        {
            applySparseCommand( applier, nodeId, usedRecord( sizeExp, nodeId ), usedRecord( sizeExp, nodeId ) );
        }

        // then
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, true ) ).isFalse();
        assertThat( idUpdates.hasEvent( sizeExp, nodeId, false ) ).isFalse();
    }

    @Test
    void shouldStartApplyingDenseCommandsOnFirstBigValueCommand() throws Exception
    {
        // given
        long nodeId = 10;
        try ( FrekiTransactionApplier applier = applier() )
        {
            TreeMap<Integer,DenseRelationships> relationships = new TreeMap<>();
            long otherNodeId = 99;
            relationships.computeIfAbsent( 0, t -> new DenseRelationships( nodeId, t ) ).add( new DenseRelationships.DenseRelationship( 1, otherNodeId, true,
                    IntObjectMaps.immutable.empty(), false ) );
            applier.handle( new FrekiCommand.DenseNode( nodeId, relationships ) );

            // At this point the dense node changes should not yet have been applied
            try ( ResourceIterator<SimpleDenseRelationshipStore.RelationshipData> readRelationships = stores.denseStore.getRelationships( nodeId,
                    ANY_RELATIONSHIP_TYPE, BOTH, NULL ) )
            {
                assertThat( readRelationships.hasNext() ).isFalse();
            }

            // when
            applier.handle( new FrekiCommand.BigPropertyValue( stores.bigPropertyValueStore.allocate( ByteBuffer.wrap( new byte[100] ), NULL ) ) );

            // then, since WorkSync works by applying itself if this is the only thread applying right now we can assert that the dense changes
            //       are now present after applying the big value.
            try ( ResourceIterator<SimpleDenseRelationshipStore.RelationshipData> readRelationships = stores.denseStore.getRelationships( nodeId,
                    ANY_RELATIONSHIP_TYPE, BOTH, NULL ) )
            {
                assertThat( readRelationships.hasNext() ).isTrue();
                assertThat( readRelationships.next().neighbourNodeId() ).isEqualTo( otherNodeId );
            }
        }
    }

    @Test
    void shouldCompleteApplyingDenseCommandsOnFirstSparseCommand() throws Exception
    {
        // given
        long nodeId = 10;
        try ( FrekiTransactionApplier applier = applier() )
        {
            TreeMap<Integer,DenseRelationships> relationships = new TreeMap<>();
            long otherNodeId = 99;
            relationships.computeIfAbsent( 0, t -> new DenseRelationships( nodeId, t ) ).add( new DenseRelationships.DenseRelationship( 1, otherNodeId, true,
                    IntObjectMaps.immutable.empty(), false ) );
            applier.handle( new FrekiCommand.DenseNode( nodeId, relationships ) );

            // At this point the dense node changes should not yet have been applied
            try ( ResourceIterator<SimpleDenseRelationshipStore.RelationshipData> readRelationships = stores.denseStore.getRelationships( nodeId,
                    ANY_RELATIONSHIP_TYPE, BOTH, NULL ) )
            {
                assertThat( readRelationships.hasNext() ).isFalse();
            }

            // when
            applySparseCommand( applier, 1, usedRecord( 0, 1 ), usedRecord( 0, 1 ) );

            // then, since WorkSync works by applying itself if this is the only thread applying right now we can assert that the dense changes
            //       are now present after applying the big value.
            try ( ResourceIterator<SimpleDenseRelationshipStore.RelationshipData> readRelationships = stores.denseStore.getRelationships( nodeId,
                    ANY_RELATIONSHIP_TYPE, BOTH, NULL ) )
            {
                assertThat( readRelationships.hasNext() ).isTrue();
                assertThat( readRelationships.next().neighbourNodeId() ).isEqualTo( otherNodeId );
            }
        }
    }

    private void applySparseCommand( FrekiTransactionApplier applier, long nodeId, Record before, Record after ) throws IOException
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
        Record record = new Record( sizeExp, id, Record.UNVERSIONED );
        record.setFlag( Record.FLAG_IN_USE, true );
        return record;
    }

    private FrekiTransactionApplier applier()
    {
        return new FrekiTransactionApplier( stores, new FrekiStorageReader( stores, NO_TRACING, null /*should not be required*/ ), null, null, EXTERNAL,
                idUpdates, null, null, denseUpdates, PageCacheTracer.NULL, NULL, EmptyMemoryTracker.INSTANCE );
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
