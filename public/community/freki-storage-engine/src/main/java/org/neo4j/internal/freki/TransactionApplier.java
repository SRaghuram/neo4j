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

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockGroup;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

class TransactionApplier extends FrekiCommand.Dispatcher.Adapter implements Visitor<StorageCommand,IOException>, AutoCloseable
{
    private final Stores stores;
    private PageCursor[] storeCursors;

    TransactionApplier( Stores stores ) throws IOException
    {
        this.stores = stores;
        this.storeCursors = stores.openMainStoreWriteCursors();
    }

    public TransactionApplier startTx( CommandsToApply batch, LockGroup locks )
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
    public void handle( FrekiCommand.DenseNode node ) throws IOException
    {
        // Thoughts: we should perhaps to the usual combine-and-apply thing for the dense node updates?
        try ( DenseStore.Updater updater = stores.denseStore.newUpdater( PageCursorTracer.NULL ) )
        {
            if ( node.inUse )
            {
                // added node properties
                node.addedProperties.forEachKeyValue( ( key, value ) -> updater.setProperty( node.nodeId, new PropertyKeyValue( key, value ) ) );
                // removed node properties
                node.removedProperties.forEach( key -> updater.removeProperty( node.nodeId, key ) );
                // created relationships
                node.createdRelationships.forEachKeyValue( ( type, typedRelationships ) ->
                        typedRelationships.forEach( relationship ->
                                updater.createRelationship( relationship.internalId, relationship.sourceNodeId, relationship.type,
                                        relationship.otherNode, relationship.outgoing, convert( relationship.properties ) ) ) );
                // deleted relationships
                node.deletedRelationships.forEachKeyValue( ( type, typedRelationships ) -> typedRelationships.forEach(
                        relationship -> updater.deleteRelationship( relationship.internalId, relationship.sourceNodeId, relationship.type, relationship.otherNode,
                                relationship.outgoing ) ) );
            }
            else
            {
                updater.deleteNode( node.nodeId );
            }
        }
    }

    // TODO this object transformation is silly, please get rid of it and instead find some unified data structure across the stack
    private Collection<StorageProperty> convert( MutableIntObjectMap<Value> properties )
    {
        Collection<StorageProperty> result = new ArrayList<>();
        properties.forEachKeyValue( ( key, value ) -> result.add( new PropertyKeyValue( key, value ) ) );
        return result;
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
    public void close() throws Exception
    {
        IOUtils.closeAll( storeCursors );
    }
}
