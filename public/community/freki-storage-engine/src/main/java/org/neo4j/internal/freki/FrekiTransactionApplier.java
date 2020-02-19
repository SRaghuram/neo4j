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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.freki.FrekiCommand.Mode;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockGroup;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

class FrekiTransactionApplier extends FrekiCommand.Dispatcher.Adapter implements Visitor<StorageCommand,IOException>, AutoCloseable
{
    private final Stores stores;
    private PageCursor[] storeCursors;
    private final IndexUpdateListener indexUpdateListener;
    private List<IndexDescriptor> createdIndexes;

    FrekiTransactionApplier( Stores stores, IndexUpdateListener indexUpdateListener ) throws IOException
    {
        this.stores = stores;
        this.storeCursors = stores.openMainStoreWriteCursors();
        this.indexUpdateListener = indexUpdateListener;
    }

    FrekiTransactionApplier startTx( CommandsToApply batch, LockGroup locks )
    {
        return this;
    }

    @Override
    public boolean visit( StorageCommand command ) throws IOException
    {
        return ((FrekiCommand) command).accept( this );
    }

    @Override
    public void handle( FrekiCommand.SparseNode node ) throws IOException
    {
        Record record = node.after();
        int sizeExp = record.sizeExp();
        stores.mainStore( sizeExp ).write( storeCursors[sizeExp], node.after() );
    }

    @Override
    public void handle( FrekiCommand.BigPropertyValue value ) throws IOException
    {
        super.handle( value );
    }

    @Override
    public void handle( FrekiCommand.DenseNode node ) throws IOException
    {
        // Thoughts: we should perhaps to the usual combine-and-apply thing for the dense node updates?
        try ( DenseStore.Updater updater = stores.denseStore.newUpdater( PageCursorTracer.NULL ) )
        {
            if ( node.inUse )
            {
                // added node properties
                node.addedProperties.forEachKeyValue( ( key, value ) -> updater.setProperty( node.nodeId, key, value ) );
                // removed node properties
                node.removedProperties.forEach( key -> updater.removeProperty( node.nodeId, key ) );
                // created relationships
                node.createdRelationships.forEachKeyValue( ( type, typedRelationships ) ->
                        typedRelationships.forEach( relationship ->
                                updater.createRelationship( relationship.internalId, relationship.sourceNodeId, type,
                                        relationship.otherNodeId, relationship.outgoing, relationship.properties ) ) );
                // deleted relationships
                node.deletedRelationships.forEachKeyValue( ( type, typedRelationships ) -> typedRelationships.forEach(
                        relationship -> updater.deleteRelationship( relationship.internalId, relationship.sourceNodeId, type,
                                relationship.otherNodeId, relationship.outgoing ) ) );
            }
            else
            {
                updater.deleteNode( node.nodeId );
            }
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
        }
        else
        {
            stores.schemaStore.deleteRule( rule.getId(), TRACER_SUPPLIER.get() );
            if ( rule instanceof IndexDescriptor  )
            {
                indexUpdateListener.dropIndex( (IndexDescriptor) rule );
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( storeCursors );
        if ( createdIndexes != null )
        {
            indexUpdateListener.createIndexes( createdIndexes.toArray( new IndexDescriptor[createdIndexes.size()] ) );
        }
    }
}
