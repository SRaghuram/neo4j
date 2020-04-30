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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.freki.FrekiCommand.Mode;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.storageengine.util.LabelIndexUpdatesWorkSync;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Record.deletedRecord;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;
import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;

class FrekiTransactionApplier extends FrekiCommand.Dispatcher.Adapter implements Visitor<StorageCommand,IOException>, AutoCloseable
{
    private final Stores stores;
    private final FrekiStorageReader reader;
    private final SchemaState schemaState;
    private final IndexUpdateListener indexUpdateListener;
    private final DenseRelationshipsWorkSync denseRelationshipsWorkSync;
    private final PageCacheTracer pageCacheTracer;
    private final PageCursorTracer cursorTracer;
    private List<IndexDescriptor> createdIndexes;
    private final IdGeneratorUpdatesWorkSync.Batch idUpdates;
    private final LabelIndexUpdatesWorkSync.Batch labelIndexUpdates;
    private final IndexUpdatesWorkSync.Batch indexUpdates;
    private final FrekiNodeCursor nodeCursor;
    private final FrekiPropertyCursor propertyCursorBefore;
    private final FrekiPropertyCursor propertyCursorAfter;
    private CountsAccessor.Updater countsApplier;
    private DenseRelationshipsWorkSync.Batch denseRelationshipUpdates;

    // State used for generating index updates
    private boolean hasAnySchema;

    FrekiTransactionApplier( Stores stores, FrekiStorageReader reader, SchemaState schemaState, IndexUpdateListener indexUpdateListener,
            TransactionApplicationMode mode, IdGeneratorUpdatesWorkSync idGeneratorUpdatesWorkSync, LabelIndexUpdatesWorkSync labelIndexUpdatesWorkSync,
            IndexUpdatesWorkSync indexUpdatesWorkSync, DenseRelationshipsWorkSync denseRelationshipsWorkSync,
            PageCacheTracer pageCacheTracer, PageCursorTracer cursorTracer )
    {
        this.stores = stores;
        this.reader = reader;
        this.schemaState = schemaState;
        this.indexUpdateListener = indexUpdateListener;
        this.denseRelationshipsWorkSync = denseRelationshipsWorkSync;
        this.pageCacheTracer = pageCacheTracer;
        this.cursorTracer = cursorTracer;
        this.labelIndexUpdates = mode == REVERSE_RECOVERY || labelIndexUpdatesWorkSync == null ? null : labelIndexUpdatesWorkSync.newBatch();
        this.idUpdates = mode == REVERSE_RECOVERY ? null : idGeneratorUpdatesWorkSync.newBatch( pageCacheTracer );
        this.indexUpdates = mode == REVERSE_RECOVERY || indexUpdatesWorkSync == null ? null : indexUpdatesWorkSync.newBatch();
        this.nodeCursor = reader.allocateNodeCursor( cursorTracer );
        this.propertyCursorBefore = reader.allocatePropertyCursor( cursorTracer );
        this.propertyCursorAfter = reader.allocatePropertyCursor( cursorTracer );
    }

    void beginTx( long transactionId )
    {
        countsApplier = stores.countsStore.apply( transactionId, cursorTracer );
        hasAnySchema = !stores.schemaCache.isEmpty();
    }

    void endTx()
    {
        closeCountsStoreApplier();
    }

    private void closeCountsStoreApplier()
    {
        if ( countsApplier != null )
        {
            countsApplier.close();
            countsApplier = null;
        }
    }

    @Override
    public boolean visit( StorageCommand command ) throws IOException
    {
        // TODO DUDE, we gotta handle reverse recovery at some point!!

        return ((FrekiCommand) command).accept( this );
    }

    @Override
    public void handle( FrekiCommand.SparseNode node ) throws IOException
    {
        for ( FrekiCommand.RecordChange change = node.changes(); change != null; change = change.next() )
        {
            int sizeExp = change.sizeExp();
            SimpleStore store = stores.mainStore( sizeExp );
            try ( PageCursor cursor = store.openWriteCursor( cursorTracer ) )
            {
                boolean updated = change.mode() == Mode.UPDATE;
                Record afterRecord = change.after != null ? change.after : deletedRecord( sizeExp, change.recordId() );
                store.write( cursor, afterRecord, idUpdates == null || updated ? IGNORE : idUpdates, cursorTracer );
            }
        }

        if ( indexUpdates != null )
        {
            checkExtractIndexUpdates( node );
        }
    }

    private void checkExtractIndexUpdates( FrekiCommand.SparseNode node )
    {
        long[] labelsBefore = findLabelsAndInitializePropertyCursor( node, propertyCursorBefore, change -> change.before, !hasAnySchema );
        long[] labelsAfter = findLabelsAndInitializePropertyCursor( node, propertyCursorAfter, change -> change.after, !hasAnySchema );
        if ( hasAnySchema )
        {
            EntityUpdates entityUpdates = extractIndexUpdates( node, labelsBefore, labelsAfter );
            Set<IndexDescriptor> relatedIndexes = stores.schemaCache.getIndexesRelatedTo( entityUpdates, EntityType.NODE );
            if ( !relatedIndexes.isEmpty() )
            {
                indexUpdates.add( entityUpdates.forIndexKeys( relatedIndexes, reader, EntityType.NODE, cursorTracer ) );
            }
        }
        if ( labelIndexUpdates != null )
        {
            labelIndexUpdates.add( EntityTokenUpdate.tokenChanges( node.nodeId, labelsBefore.clone(), labelsAfter.clone() ) );
        }
    }

    public EntityUpdates extractIndexUpdates( FrekiCommand.SparseNode node, long[] labelsBefore, long[] labelsAfter )
    {
        FrekiCommand.RecordChange x1 = node.change( 0 );
        boolean nodeIsCreatedRightNow = x1 != null && x1.mode() == Mode.CREATE;

        EntityUpdates.Builder builder = EntityUpdates.forEntity( node.nodeId, nodeIsCreatedRightNow );
        builder.withTokens( labelsBefore ).withTokensAfter( labelsAfter );
        boolean beforeHasNext = propertyCursorBefore.next();
        boolean afterHasNext = propertyCursorAfter.next();
        int prevBeforeKey = -1;
        int prevAfterKey = -1;
        while ( beforeHasNext || afterHasNext )
        {
            int beforeKey = beforeHasNext ? propertyCursorBefore.propertyKey() : Integer.MAX_VALUE;
            int afterKey = afterHasNext ? propertyCursorAfter.propertyKey() : Integer.MAX_VALUE;

            assert !beforeHasNext || prevBeforeKey == -1 || prevBeforeKey <= beforeKey;
            assert !afterHasNext || prevAfterKey == -1 || prevAfterKey <= afterKey;
            prevBeforeKey = beforeKey;
            prevAfterKey = afterKey;

            if ( beforeKey < afterKey )
            {
                // a property has been removed
                builder.removed( beforeKey, propertyCursorBefore.propertyValue() );
                beforeHasNext = propertyCursorBefore.next();
            }
            else if ( beforeKey > afterKey )
            {
                // a property has been added
                builder.added( afterKey, propertyCursorAfter.propertyValue() );
                afterHasNext = propertyCursorAfter.next();
            }
            else
            {
                // a property has been changed or is unchanged
                Value beforeValue = propertyCursorBefore.propertyValue();
                Value afterValue = propertyCursorAfter.propertyValue();
                if ( !beforeValue.equals( afterValue ) )
                {
                    builder.changed( afterKey, beforeValue, afterValue );
                }
                beforeHasNext = propertyCursorBefore.next();
                afterHasNext = propertyCursorAfter.next();
            }
        }
        return builder.build();
    }

    private long[] findLabelsAndInitializePropertyCursor( FrekiCommand.SparseNode node, FrekiPropertyCursor propertyCursor,
            Function<FrekiCommand.RecordChange,Record> recordFunction, boolean skipProperties )
    {
        long[] labels = EMPTY_LONG_ARRAY;
        boolean investigatedX1 = false;
        boolean investigatedXL = false;
        boolean labelsLoaded = false;
        boolean propertiesLoaded = skipProperties;
        for ( FrekiCommand.RecordChange change = node.changes(); change != null && (!propertiesLoaded || !labelsLoaded); change = change.next() )
        {
            Record record = recordFunction.apply( change );
            boolean inUse = record != null;
            if ( inUse )
            {
                nodeCursor.initializeFromRecord( record );
                if ( !labelsLoaded )
                {
                    if ( nodeCursor.data.header.hasMark( Header.FLAG_LABELS ) )
                    {
                        labels = nodeCursor.labels();
                        labelsLoaded = true;
                    }
                    else if ( !nodeCursor.data.header.hasReferenceMark( Header.FLAG_LABELS ) )
                    {
                        labelsLoaded = true;
                    }
                }
                if ( !propertiesLoaded )
                {
                    if ( nodeCursor.data.header.hasMark( Header.OFFSET_PROPERTIES ) )
                    {
                        nodeCursor.properties( propertyCursor );
                        propertiesLoaded = true;
                    }
                    else if ( !nodeCursor.data.header.hasReferenceMark( Header.OFFSET_PROPERTIES ) )
                    {
                        propertyCursor.reset();
                        propertiesLoaded = true;
                    }
                }
            }

            if ( change.sizeExp() == 0 )
            {
                //in x1
                investigatedX1 = true;
                if ( !inUse || nodeCursor.data.forwardPointer == NULL )
                {
                    investigatedXL = true;
                    break;
                }
            }
            else
            {
                investigatedXL = true;
                if ( inUse )
                {
                    break;
                }
            }
        }

        if ( (!propertiesLoaded || !labelsLoaded) && (!investigatedX1 || !investigatedXL) )
        {
            nodeCursor.single( node.nodeId );
            if ( nodeCursor.next() )
            {
                if ( !labelsLoaded )
                {
                    labels = nodeCursor.labels();
                }
                if ( !propertiesLoaded )
                {
                    nodeCursor.properties( propertyCursor );
                    propertiesLoaded = true;
                }
            }
        }

        if ( !propertiesLoaded )
        {
            propertyCursor.reset();
        }
        return labels;
    }

    @Override
    public void handle( FrekiCommand.BigPropertyValue value ) throws IOException
    {
        try ( PageCursor cursor = stores.bigPropertyValueStore.openWriteCursor( cursorTracer ) )
        {
            stores.bigPropertyValueStore.write( cursor, ByteBuffer.wrap( value.bytes, 0, value.length ), value.pointer );
        }
    }

    @Override
    public void handle( FrekiCommand.DenseNode node ) throws IOException
    {
        if ( denseRelationshipUpdates == null )
        {
            denseRelationshipUpdates = denseRelationshipsWorkSync.newBatch();
        }
        denseRelationshipUpdates.add( node );
    }

    @Override
    public void handle( FrekiCommand.LabelToken token ) throws IOException
    {
        stores.labelTokenStore.writeToken( token.token, cursorTracer );
    }

    @Override
    public void handle( FrekiCommand.RelationshipTypeToken token ) throws IOException
    {
        stores.relationshipTypeTokenStore.writeToken( token.token, cursorTracer );
    }

    @Override
    public void handle( FrekiCommand.PropertyKeyToken token ) throws IOException
    {
        stores.propertyKeyTokenStore.writeToken( token.token, cursorTracer );
    }

    @Override
    public void handle( FrekiCommand.Schema schema ) throws IOException
    {
        // TODO There should be logic around avoiding deadlocks and working around problems with batches of transactions where some transactions
        //      updates indexes and some change schema, which are not quite implemented here yet

        // (copied from RecordStorageEngine) closing the counts store here resolves an otherwise deadlocking scenario between check pointer,
        // this applier and an index population thread wanting to apply index sampling to the counts store.
        closeCountsStoreApplier();

        SchemaRule rule = schema.descriptor;
        if ( schema.mode == Mode.CREATE || schema.mode == Mode.UPDATE )
        {
            stores.schemaStore.writeRule( rule, cursorTracer );
            if ( rule instanceof IndexDescriptor )
            {
                if ( schema.mode == Mode.CREATE )
                {
                    if ( createdIndexes == null )
                    {
                        createdIndexes = new ArrayList<>();
                    }
                    createdIndexes.add( (IndexDescriptor) rule );
                }
                else
                {
                    try
                    {
                        indexUpdateListener.activateIndex( (IndexDescriptor) rule );
                    }
                    catch ( KernelException e )
                    {
                        throw new IllegalStateException( e );
                    }
                }
            }
            stores.schemaCache.addSchemaRule( rule );
            if ( rule instanceof ConstraintDescriptor )
            {
                schemaState.clear();
            }
        }
        else
        {
            if ( stores.schemaStore.deleteRule( rule.getId(), cursorTracer ) )
            {
                if ( rule instanceof IndexDescriptor )
                {
                    indexUpdateListener.dropIndex( (IndexDescriptor) rule );
                }
                stores.schemaCache.removeSchemaRule( rule.getId() );
                schemaState.clear();
            }
        }
    }

    @Override
    public void handle( FrekiCommand.NodeCount count ) throws IOException
    {
        countsApplier.incrementNodeCount( count.labelId, count.count );
    }

    @Override
    public void handle( FrekiCommand.RelationshipCount count ) throws IOException
    {
        countsApplier.incrementRelationshipCount( count.startLabelId, count.typeId, count.endLabelId, count.count );
    }

    @Override
    public void close() throws Exception
    {
        AutoCloseable indexesCloser = () ->
        {
            if ( createdIndexes != null )
            {
                indexUpdateListener.createIndexes( createdIndexes.toArray( new IndexDescriptor[createdIndexes.size()] ) );
            }
            AsyncApply labelUpdatesAsyncApply = labelIndexUpdates != null ? labelIndexUpdates.applyAsync( cursorTracer ) : AsyncApply.EMPTY;
            AsyncApply indexUpdatesAsyncApply = indexUpdates != null ? indexUpdates.applyAsync( cursorTracer ) : AsyncApply.EMPTY;
            AsyncApply denseAsyncApply = denseRelationshipUpdates != null ? denseRelationshipUpdates.applyAsync( pageCacheTracer ) : AsyncApply.EMPTY;
            if ( idUpdates != null )
            {
                idUpdates.apply( pageCacheTracer );
            }
            labelUpdatesAsyncApply.await();
            indexUpdatesAsyncApply.await();
            denseAsyncApply.await();
        };
        closeAll( nodeCursor, propertyCursorBefore, propertyCursorAfter, indexesCloser );
    }
}
