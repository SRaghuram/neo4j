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

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.storageengine.util.LabelIndexUpdatesWorkSync;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.PropertyValueFormat.read;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;
import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;

class FrekiTransactionApplier extends FrekiCommand.Dispatcher.Adapter implements Visitor<StorageCommand,IOException>, AutoCloseable
{
    private final Stores stores;
    private final SchemaState schemaState;
    private final IndexUpdateListener indexUpdateListener;
    private List<IndexDescriptor> createdIndexes;
    private final IdGeneratorUpdatesWorkSync.Batch idUpdates;
    private final LabelIndexUpdatesWorkSync.Batch labelIndexUpdates;
    private final IndexUpdatesWorkSync.Batch indexUpdates;
    private final FrekiNodeCursor nodeCursor;
    private final FrekiPropertyCursor propertyCursorBefore;
    private final FrekiPropertyCursor propertyCursorAfter;
    private CountsAccessor.Updater countsApplier;

    // State used for generating index updates
    private long currentNodeId = NULL;
    private final FrekiCommand.SparseNode[] currentSparseNodeCommands = new FrekiCommand.SparseNode[4];
    private FrekiCommand.DenseNode currentDenseNodeCommand;

    FrekiTransactionApplier( Stores stores, SchemaState schemaState, IndexUpdateListener indexUpdateListener, TransactionApplicationMode mode,
            IdGeneratorUpdatesWorkSync idGeneratorUpdatesWorkSync, LabelIndexUpdatesWorkSync labelIndexUpdatesWorkSync,
            IndexUpdatesWorkSync indexUpdatesWorkSync )
    {
        this.stores = stores;
        this.schemaState = schemaState;
        this.indexUpdateListener = indexUpdateListener;
        this.labelIndexUpdates = mode == REVERSE_RECOVERY || labelIndexUpdatesWorkSync == null ? null : labelIndexUpdatesWorkSync.newBatch();
        this.idUpdates = mode == REVERSE_RECOVERY ? null : idGeneratorUpdatesWorkSync.newBatch();
        this.indexUpdates = mode == REVERSE_RECOVERY || indexUpdatesWorkSync == null ? null : indexUpdatesWorkSync.newBatch();
        this.nodeCursor = new FrekiNodeCursor( stores, PageCursorTracer.NULL );
        this.propertyCursorBefore = new FrekiPropertyCursor( stores, PageCursorTracer.NULL );
        this.propertyCursorAfter = new FrekiPropertyCursor( stores, PageCursorTracer.NULL );
    }

    void beginTx( long transactionId )
    {
        countsApplier = stores.countsStore.apply( transactionId, PageCursorTracer.NULL );
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
        checkExtractIndexUpdates( node.nodeId );

        Record record = node.after;
        int sizeExp = record.sizeExp();
        SimpleStore store = stores.mainStore( sizeExp );
        try ( PageCursor cursor = store.openWriteCursor() )
        {
            store.write( cursor, node.after, idUpdates != null ? idUpdates : IGNORE, PageCursorTracer.NULL );
        }
        // OK so this logic here is quite specific to the format, it knows that labels are kept in the smallest records only and
        // reads label information from it and hands off to the label index updates listener.
        if ( sizeExp == 0 && labelIndexUpdates != null )
        {
            labelIndexUpdates.add( NodeLabelUpdate.labelChanges( node.after.id, parseLabels( node.before ), parseLabels( node.after ) ) );
        }

        if ( indexUpdates != null )
        {
            currentSparseNodeCommands[sizeExp] = node;
        }
    }

    private void checkExtractIndexUpdates( long nodeId )
    {
        if ( nodeId != currentNodeId )
        {
            if ( currentNodeId != NULL )
            {
                EntityUpdates entityUpdates = extractIndexUpdates();
                Set<IndexDescriptor> relatedIndexes = stores.schemaCache.getIndexesRelatedTo( entityUpdates, EntityType.NODE );
                if ( !relatedIndexes.isEmpty() )
                {
                    indexUpdates.add( entityUpdates.forIndexKeys( relatedIndexes, null, EntityType.NODE, PageCursorTracer.NULL ) );
                }
            }

            Arrays.fill( currentSparseNodeCommands, null );
            currentDenseNodeCommand = null;
            currentNodeId = nodeId;
        }
    }

    public EntityUpdates extractIndexUpdates()
    {
        boolean propertiesHandledByDenseNode = currentDenseNodeCommand != null;
        long[] labelsBefore;
        long[] labelsAfter;
        FrekiCommand.SparseNode smallNode = currentSparseNodeCommands[0];
        boolean nodeIsCreatedRightNow = false;
        if ( smallNode != null )
        {
            labelsBefore = parseLabels( smallNode.before );
            labelsAfter = parseLabels( smallNode.after );
            if ( !propertiesHandledByDenseNode )
            {
                initializePropertyCursorOnRecord( propertyCursorBefore, findRelevantUsedRecord( node -> node.before ) );
                initializePropertyCursorOnRecord( propertyCursorAfter, findRelevantUsedRecord( node -> node.after ) );
            }
            nodeIsCreatedRightNow = !smallNode.before.hasFlag( FLAG_IN_USE );
        }
        else
        {
            // TODO loading the node like this will also have the cursor load the larger record, can this be avoided?
            //      this could however be a general cursor issue where we'd typically want to lazy-load that bigger record, no?
            nodeCursor.single( currentNodeId );
            Preconditions.checkState( nodeCursor.next(), "Node %d didn't exist", currentNodeId );
            labelsBefore = nodeCursor.labels();
            labelsAfter = labelsBefore;

            if ( !propertiesHandledByDenseNode )
            {
                // So since we loaded this record from store we might as well use it to initialize the after-property cursor,
                // because they should have the same contents at this point
                nodeCursor.properties( propertyCursorAfter );
                // Either this node was just now created, or this node was already large and the large record was updated
                boolean beforeFound = false;
                for ( FrekiCommand.SparseNode nodeCommand : currentSparseNodeCommands )
                {
                    if ( nodeCommand != null && nodeCommand.before.hasFlag( FLAG_IN_USE ) )
                    {
                        initializePropertyCursorOnRecord( propertyCursorBefore, nodeCommand.before );
                        beforeFound = true;
                        break;
                    }
                }
                if ( !beforeFound )
                {
                    propertyCursorBefore.reset();
                }
            }
        }

        EntityUpdates.Builder builder = EntityUpdates.forEntity( currentNodeId, nodeIsCreatedRightNow );
        builder.withTokens( labelsBefore ).withTokensAfter( labelsAfter );
        if ( !propertiesHandledByDenseNode )
        {
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
                    if ( beforeValue.equals( afterValue ) )
                    {
                        builder.existing( afterKey, afterValue );
                    }
                    else
                    {
                        builder.changed( afterKey, beforeValue, afterValue );
                    }
                    beforeHasNext = propertyCursorBefore.next();
                    afterHasNext = propertyCursorAfter.next();
                }
            }
        }
        else
        {
            //We need to go over all properties (load from store), to find unchanged
            Record before = findRelevantUsedRecord( node -> node.before );
            if ( before == null )
            {
                propertyCursorBefore.initNodeProperties( FrekiReference.nodeReference( currentDenseNodeCommand.nodeId ) );
            }
            else
            {
                initializePropertyCursorOnRecord( propertyCursorBefore, before );
            }

            MutableIntSet newProperties = IntSets.mutable.empty();
            for ( PropertyUpdate propertyUpdate : currentDenseNodeCommand.propertyUpdates )
            {
                if ( propertyUpdate.mode == Mode.CREATE )
                {
                    newProperties.add( propertyUpdate.propertyKeyId );
                }
            }
            while ( propertyCursorBefore.next() )
            {
                int key = propertyCursorBefore.propertyKey();
                PropertyUpdate update = currentDenseNodeCommand.propertyUpdates.get( key );
                if ( update != null )
                {
                    if ( update.mode == Mode.CREATE || update.mode == Mode.UPDATE )
                    {
                        newProperties.remove( key );
                        builder.changed( key, propertyCursorBefore.propertyValue(), readValueAndRestorePosition( update.after ) );
                    }
                    else
                    {
                        builder.removed( key, propertyCursorBefore.propertyValue() );
                    }
                }
                else
                {
                    builder.existing( key, propertyCursorBefore.propertyValue() );
                }
            }
            newProperties.forEach(
                    key -> builder.added( key, readValueAndRestorePosition( currentDenseNodeCommand.propertyUpdates.get( key ).after ) ) );
        }
        return builder.build();
    }

    private Value readValueAndRestorePosition( ByteBuffer buffer )
    {
        int position = buffer.position();
        try
        {
            return read( buffer, stores.bigPropertyValueStore );
        }
        finally
        {
            buffer.position( position );
        }
    }

    private Record findRelevantUsedRecord( Function<FrekiCommand.SparseNode,Record> recordFunction )
    {
        Record smallRecord = recordFunction.apply( currentSparseNodeCommands[0] );
        if ( smallRecord.hasFlag( FLAG_IN_USE ) )
        {
            nodeCursor.initializeFromRecord( smallRecord );
            if ( nodeCursor.headerState.isDense )
            {
                return null;
            }
            return !nodeCursor.headerState.containsForwardPointer
                   ? smallRecord
                   : recordFunction.apply( currentSparseNodeCommands[sizeExponentialFromForwardPointer( nodeCursor.headerState.forwardPointer )] );
        }
        return null;
    }

    private void initializePropertyCursorOnRecord( FrekiPropertyCursor propertyCursor, Record record )
    {
        if ( record != null && nodeCursor.initializeFromRecord( record ) )
        {
            nodeCursor.properties( propertyCursor );
        }
        else
        {
            propertyCursor.reset();
        }
    }

    private long[] parseLabels( Record record )
    {
        return nodeCursor.initializeFromRecord( record ) ? nodeCursor.labels() : EMPTY_LONG_ARRAY;
    }

    @Override
    public void handle( FrekiCommand.BigPropertyValue value ) throws IOException
    {
        try ( PageCursor cursor = stores.bigPropertyValueStore.openWriteCursor() )
        {
            stores.bigPropertyValueStore.write( cursor, ByteBuffer.wrap( value.bytes ), value.pointer );
        }
    }

    @Override
    public void handle( FrekiCommand.DenseNode node ) throws IOException
    {
        checkExtractIndexUpdates( node.nodeId ); //apply for prev
        currentDenseNodeCommand = node;
        checkExtractIndexUpdates( NULL ); // apply for this

        // Thoughts: we should perhaps to the usual combine-and-apply thing for the dense node updates?
        try ( DenseStore.Updater updater = stores.denseStore.newUpdater( PageCursorTracer.NULL ) )
        {
            // node properties
            node.propertyUpdates.forEach( p ->
            {
                if ( p.mode == Mode.CREATE || p.mode == Mode.UPDATE )
                {
                    updater.setProperty( node.nodeId, p.propertyKeyId, p.after );
                }
                else
                {
                    updater.removeProperty( node.nodeId, p.propertyKeyId );
                }
            } );
            // relationships
            node.relationshipUpdates.forEachKeyValue( ( type, typedRelationships ) ->
            {
                typedRelationships.deleted.forEach( relationship ->
                        updater.deleteRelationship( relationship.internalId, node.nodeId, type, relationship.otherNodeId, relationship.outgoing ) );
                typedRelationships.created.forEach( relationship -> updater.createRelationship( relationship.internalId, node.nodeId, type,
                        relationship.otherNodeId, relationship.outgoing, relationship.propertyUpdates, u -> u.after ) );
                DenseRelationships.Degree degree = typedRelationships.degree( true );
                updater.setDegree( node.nodeId, type, degree.outgoing(), degree.incoming(), degree.loop() );
            } );
        }
    }

    @Override
    public void handle( FrekiCommand.LabelToken token ) throws IOException
    {
        stores.labelTokenStore.writeToken( token.token, TRACER_SUPPLIER.get() );
    }

    @Override
    public void handle( FrekiCommand.RelationshipTypeToken token ) throws IOException
    {
        stores.relationshipTypeTokenStore.writeToken( token.token, TRACER_SUPPLIER.get() );
    }

    @Override
    public void handle( FrekiCommand.PropertyKeyToken token ) throws IOException
    {
        stores.propertyKeyTokenStore.writeToken( token.token, TRACER_SUPPLIER.get() );
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
            stores.schemaStore.writeRule( rule, TRACER_SUPPLIER.get() );
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
            if ( stores.schemaStore.deleteRule( rule.getId(), TRACER_SUPPLIER.get() ) )
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
            AsyncApply labelUpdatesAsyncApply = labelIndexUpdates != null ? labelIndexUpdates.applyAsync() : AsyncApply.EMPTY;
            AsyncApply indexUpdatesAsyncApply = indexUpdates != null ? indexUpdates.applyAsync( PageCursorTracer.NULL ) : AsyncApply.EMPTY;
            if ( idUpdates != null )
            {
                idUpdates.apply();
            }
            labelUpdatesAsyncApply.await();
            indexUpdatesAsyncApply.await();
        };
        closeAll( () -> checkExtractIndexUpdates( NULL ), nodeCursor, propertyCursorBefore, propertyCursorAfter, indexesCloser );
    }
}
