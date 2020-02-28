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

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Consumer;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.freki.FrekiCommand.Mode;
import org.neo4j.internal.freki.MutableNodeRecordData.Relationship;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.isDenseFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.helpers.collection.Iterators.loop;

class CommandCreator implements TxStateVisitor
{
    private final Collection<StorageCommand> commands;
    private final Stores stores;
    private final MutableLongObjectMap<Mutation> mutations = LongObjectMaps.mutable.empty();

    CommandCreator( Collection<StorageCommand> commands, Stores stores )
    {
        this.commands = commands;
        this.stores = stores;
    }

    @Override
    public void visitCreatedNode( long id )
    {
        mutations.put( id, createNew( stores.mainStore, id ) );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        getOrLoad( id ).markAsUnused();
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode, Iterable<StorageProperty> addedProperties )
    {
        // TODO we cannot use the provided id since it comes from kernel which generated a relationship ID from an IdGenerator.
        //      The actual ID that this relationship will get is something else, something based on the node ID and other data found in the node itself.
        //      This is going to require some changes in the kernel, or at least some restrictions on how the Core API can expect to make use of
        //      relationship IDs that gets created inside a transaction (the ID of the relationship read from the store later on will be different.

        Relationship relationshipAtStartNode = createRelationship( null, type, startNode, endNode, addedProperties );
        if ( startNode != endNode )
        {
            createRelationship( relationshipAtStartNode, type, endNode, startNode, addedProperties );
        }
    }

    private Relationship createRelationship( Relationship sourceNodeRelationship, int type, long sourceNode, long targetNode,
            Iterable<StorageProperty> addedProperties )
    {
        Mutation sourceMutation = getOrLoad( sourceNode );
        Relationship relationship = sourceMutation.current.createRelationship( sourceNodeRelationship, targetNode, type );
        for ( StorageProperty property : addedProperties )
        {
            relationship.addProperty( property.propertyKeyId(), property.value() );
        }
        return relationship;
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        Mutation mutation = getOrLoad( id );
        setNodeProperties( added, mutation );
        setNodeProperties( changed, mutation );
        removed.forEach( propertyKey -> mutation.current.removeNodeProperty( propertyKey ) );
    }

    private void setNodeProperties( Iterable<StorageProperty> added, Mutation mutation )
    {
        for ( StorageProperty property : added )
        {
            mutation.current.setNodeProperty( property.propertyKeyId(), property.value() );
        }
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
    {
        Mutation mutation = getOrLoad( id );
        mutation.small.addLabels( added );
        mutation.small.removeLabels( removed );
    }

    @Override
    public void visitAddedIndex( IndexDescriptor element )
    {
        commands.add( new FrekiCommand.Schema( element, Mode.CREATE ) );
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor element )
    {
        commands.add( new FrekiCommand.Schema( element, Mode.DELETE ) );
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor constraint ) throws KernelException
    {
        constraint = constraint.withId( stores.schemaStore.nextSchemaRuleId( PageCursorTracer.NULL ) );
        commands.add( new FrekiCommand.Schema( constraint, Mode.CREATE ) );
        switch ( constraint.type() )
        {
        case UNIQUE:
            // This also means updating the index to have this constraint as owner
            IndexDescriptor index = (IndexDescriptor) stores.schemaStore.loadRule( constraint.asUniquenessConstraint().ownedIndexId(), PageCursorTracer.NULL );
            commands.add( new FrekiCommand.Schema( index.withOwningConstraintId( constraint.getId() ), Mode.UPDATE ) );
            break;
        default:
            throw new UnsupportedOperationException( "Not implemented yet" );
        }
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor constraint )
    {
        commands.add( new FrekiCommand.Schema( constraint, Mode.DELETE ) );
        switch ( constraint.type() )
        {
        case UNIQUE:
            // Remove the index for the constraint as well
            Iterator<IndexDescriptor> indexes = stores.schemaCache.indexesForSchema( constraint.schema() );
            for ( IndexDescriptor index : loop( indexes ) )
            {
                OptionalLong owningConstraintId = index.getOwningConstraintId();
                if ( owningConstraintId.isPresent() && owningConstraintId.getAsLong() == constraint.getId() )
                {
                    visitRemovedIndex( index );
                }
                // Note that we _could_ also go through all the matching indexes that have isUnique == true and no owning constraint id, and remove those
                // as well. These might be orphaned indexes from failed constraint creations. However, since we want to allow multiple indexes and
                // constraints on the same schema, they could also be constraint indexes that are currently populating for other constraints, and if that's
                // the case, then we cannot remove them, since that would ruin the constraint they are being built for.
            }
            break;
        default:
            throw new UnsupportedOperationException( "Not implemented yet" );
        }
    }

    @Override
    public void visitCreatedLabelToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.LabelToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void visitCreatedPropertyKeyToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.PropertyKeyToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void visitCreatedRelationshipTypeToken( long id, String name, boolean internal )
    {
        commands.add( new FrekiCommand.RelationshipTypeToken( new NamedToken( name, toIntExact( id ), internal ) ) );
    }

    @Override
    public void close()
    {
        List<StorageCommand> bigValueCommands = new ArrayList<>();
        List<StorageCommand> otherCommands = new ArrayList<>();

        mutations.each( mutation ->
        {
            RecordAndData abandonedLargeRecord = null;
            while ( true )
            {
                try
                {
                    mutation.current.prepareForCommandExtraction( bigValueCommands::add );
                    break;
                }
                catch ( BufferOverflowException | ArrayIndexOutOfBoundsException e )
                {
                    // TODO a silly thing to do, but catching this here makes this temporarily very simple to detect and grow into a larger record
                    RecordAndData largerRecord = mutation.current.growAndRelocate();
                    largerRecord.markInUse( true );
                    largerRecord.markCreated();
                    if ( mutation.large != null && !mutation.large.isCreated() && abandonedLargeRecord == null )
                    {
                        // Keep this abandoned large record around because we have to remove it below
                        abandonedLargeRecord = mutation.large;
                        abandonedLargeRecord.markInUse( false );
                    }
                    mutation.large = mutation.current = largerRecord;
                }
            }
            if ( mutation.current != mutation.small )
            {
                // The record have grown into a larger record, although still prepare the small one due to potential changes
                // TODO we should track whether or not we need to do this preparation actually
                // TODO for the time being we expect this to work because there should only be labels in it, 60 or so should fit
                mutation.small.data.setForwardPointer( mutation.current.asForwardPointer() );
                mutation.small.prepareForCommandExtraction( bigValueCommands::add );
            }

            mutation.small.createCommands( otherCommands ); //Small
            if ( abandonedLargeRecord != null ) //Large
            {
                abandonedLargeRecord.createCommands( otherCommands );
            }
            if ( mutation.large != null ) //Dense
            {
                mutation.large.createCommands( otherCommands );
            }
        } );

        commands.addAll( bigValueCommands );
        commands.addAll( otherCommands );
    }

    private Mutation createNew( SimpleStore store, long id )
    {
        Mutation mutation = new Mutation();
        mutation.small = new SparseRecordAndData( stores, store, id, null );
        mutation.small.markInUse( true );
        mutation.small.markCreated();
        mutation.current = mutation.small;
        return mutation;
    }

    private Mutation getOrLoad( long id )
    {
        Mutation mutation = mutations.get( id );
        if ( mutation == null )
        {
            mutation = new Mutation();
            mutation.small = new SparseRecordAndData( stores, stores.mainStore, id, null );
            mutation.small.loadExistingData();
            mutation.current = mutation.small;
            long forwardPointer = mutation.small.getForwardPointer();
            if ( forwardPointer != NULL )
            {
                // At this point it can still be either a larger record or a dense representation (GBPTree)
                boolean isDense = isDenseFromForwardPointer( forwardPointer );
                if ( !isDense )
                {
                    int fwSizeExp = sizeExponentialFromForwardPointer( forwardPointer );
                    long fwId = idFromForwardPointer( forwardPointer );
                    SimpleStore largeStore = stores.mainStore( fwSizeExp );
                    mutation.large = new SparseRecordAndData( stores, largeStore, fwId, mutation.small );
                }
                else
                {
                    mutation.large = new DenseRecordAndData( stores.denseStore, stores.bigPropertyValueStore, mutation.small );
                }
                mutation.current = mutation.large;
                mutation.current.loadExistingData();
            }
            mutations.put( id, mutation );
        }
        return mutation;
    }

    private static class Mutation
    {
        private SparseRecordAndData small;
        private RecordAndData large;
        // current points to either or
        private RecordAndData current;

        void markAsUnused()
        {
            small.markInUse( false );
            current.markInUse( false );
        }
    }

    private abstract static class RecordAndData
    {
        abstract void markCreated();

        abstract boolean isCreated();

        abstract void markInUse( boolean inUse );

        abstract void loadExistingData();

        abstract Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type );

        abstract void setNodeProperty( int propertyKeyId, Value value );

        abstract void removeNodeProperty( int propertyKeyId );

        abstract void addLabels( LongSet added );

        abstract void removeLabels( LongSet removed );

        abstract void prepareForCommandExtraction( Consumer<StorageCommand> bigValueCommandConsumer );

        abstract RecordAndData growAndRelocate();

        abstract long asForwardPointer();

        abstract void createCommands( Collection<StorageCommand> commands );
    }

    private static class SparseRecordAndData extends RecordAndData
    {
        private final MainStores stores;
        private final SimpleStore store;
        private final SparseRecordAndData smallRecord;

        private boolean created;
        private Record before;
        private Record after;
        // data here is really accidental state, a deserialized objectified version of the data found in the byte[] of the "after" record
        private MutableNodeRecordData data;

        SparseRecordAndData( MainStores stores, SimpleStore store, long id, SparseRecordAndData smallRecord )
        {
            this.stores = stores;
            this.store = store;
            this.smallRecord = smallRecord != null ? smallRecord : this;
            int sizeExp = store.recordSizeExponential();
            before = new Record( sizeExp, id );
            after = new Record( sizeExp, id );
            data = new MutableNodeRecordData( id );
            if ( smallRecord != null )
            {
                data.setBackPointer( smallRecord.data.id );
            }
        }

        @Override
        void markCreated()
        {
            created = true;
        }

        @Override
        public boolean isCreated()
        {
            return created;
        }

        @Override
        void markInUse( boolean inUse )
        {
            after.setFlag( FLAG_IN_USE, inUse );
        }

        @Override
        void loadExistingData()
        {
            try ( PageCursor cursor = store.openReadCursor() )
            {
                store.read( cursor, before, after.id );
            }
            data.deserialize( before.dataForReading(), stores.bigPropertyValueStore );
            after.copyContentsFrom( before );
        }

        @Override
        Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type )
        {
            return data.createRelationship( sourceNodeRelationship, targetNode, type );
        }

        @Override
        void setNodeProperty( int propertyKeyId, Value value )
        {
            data.setNodeProperty( propertyKeyId, value );
        }

        @Override
        void removeNodeProperty( int propertyKeyId )
        {
            data.removeNodeProperty( propertyKeyId );
        }

        @Override
        void addLabels( LongSet added )
        {
            added.forEach( label -> data.labels.add( toIntExact( label ) ) );
        }

        @Override
        void removeLabels( LongSet removed )
        {
            removed.forEach( label -> data.labels.remove( toIntExact( label ) ) );
        }

        long getForwardPointer()
        {
            return data.getForwardPointer();
        }

        @Override
        void prepareForCommandExtraction( Consumer<StorageCommand> bigValueCommandConsumer )
        {
            data.serialize( after.dataForWriting(), stores.bigPropertyValueStore, bigValueCommandConsumer );
            if ( data.id == -1 )
            {
                acquireId();
            }
        }

        @Override
        RecordAndData growAndRelocate()
        {
            SimpleStore largerStore = stores.nextLargerMainStore( after.sizeExp() );
            RecordAndData result;
            if ( largerStore != null )
            {
                SparseRecordAndData largerRecord = new SparseRecordAndData( stores, largerStore, -1, smallRecord );
                data.movePropertiesAndRelationshipsTo( largerRecord.data );
                result = largerRecord;
            }
            else
            {
                // Time to move over to GBPTree-style data
                DenseRecordAndData denseRecord = new DenseRecordAndData( stores.denseStore, stores.bigPropertyValueStore, smallRecord );
                denseRecord.movePropertiesAndRelationshipsFrom( data );
                result = denseRecord;
            }
            return result;
        }

        private void acquireId()
        {
            long id = store.nextId( PageCursorTracer.NULL );
            data.id = id;
            before.id = id;
            after.id = id;
        }

        @Override
        long asForwardPointer()
        {
            return forwardPointer( store.recordSizeExponential(), false, after.id );
        }

        @Override
        void createCommands( Collection<StorageCommand> commands )
        {
            commands.add( new FrekiCommand.SparseNode( smallRecord.data.id, before, after ) );
        }
    }

    private static class DenseRecordAndData extends RecordAndData
    {
        // meta
        private final DenseStore store;
        private final SimpleBigValueStore bigValueStore;
        private final SparseRecordAndData smallRecord;
        private boolean created;
        private boolean inUse;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private MutableIntObjectMap<MutableNodeRecordData.Relationships> createdRelationships = IntObjectMaps.mutable.empty();
        private MutableIntObjectMap<MutableNodeRecordData.Relationships> deletedRelationships = IntObjectMaps.mutable.empty();
        private MutableIntObjectMap<Value> addedProperties = IntObjectMaps.mutable.empty();
        private MutableIntSet removedProperties = IntSets.mutable.empty();

        DenseRecordAndData( DenseStore store, SimpleBigValueStore bigValueStore, SparseRecordAndData smallRecord )
        {
            this.store = store;
            this.bigValueStore = bigValueStore;
            this.smallRecord = smallRecord;
        }

        @Override
        void markCreated()
        {
            created = true;
        }

        @Override
        boolean isCreated()
        {
            return created;
        }

        @Override
        void markInUse( boolean inUse )
        {
            this.inUse = inUse;
        }

        @Override
        void loadExistingData()
        {
            // Thoughts: we shouldn't really load anything here since we don't need the existing data in order to make changes
            markInUse( true );
        }

        @Override
        Relationship createRelationship( Relationship sourceNodeRelationship, long targetNode, int type )
        {
            long internalRelationshipId = sourceNodeRelationship == null
                    ? smallRecord.data.nextInternalRelationshipId()
                    : sourceNodeRelationship.internalId;
            boolean outgoing = sourceNodeRelationship == null;
            return createdRelationships.getIfAbsentPutWithKey( type, MutableNodeRecordData.Relationships::new )
                    .add( internalRelationshipId, smallRecord.after.id, targetNode, type, outgoing );
        }

        @Override
        void setNodeProperty( int propertyKeyId, Value value )
        {
            removedProperties.remove( propertyKeyId );
            addedProperties.put( propertyKeyId, value );
        }

        @Override
        void removeNodeProperty( int propertyKeyId )
        {
            addedProperties.remove( propertyKeyId );
            removedProperties.add( propertyKeyId );
        }

        @Override
        void addLabels( LongSet added )
        {
            throw new UnsupportedOperationException( "Should not happen, right?" );
        }

        @Override
        void removeLabels( LongSet removed )
        {
            throw new UnsupportedOperationException( "Should not happen, right?" );
        }

        @Override
        void prepareForCommandExtraction( Consumer<StorageCommand> bigValueCommandConsumer )
        {
            // Thoughts: no special preparation should be required here either
        }

        @Override
        RecordAndData growAndRelocate()
        {
            throw new UnsupportedOperationException( "There should be no need to grow into something bigger" );
        }

        @Override
        long asForwardPointer()
        {
            // Thoughts: the nodeId comes into play when we have node-centric trees, where the root id can be the nodeId here
            return forwardPointer( 0, true, 0 );
        }

        @Override
        void createCommands( Collection<StorageCommand> commands )
        {
            // Thoughts: take all the logical changes and add to logical commands, either separate for each change or one big command containing all
            commands.add(
                    new FrekiCommand.DenseNode( smallRecord.after.id, inUse, convertProperties( addedProperties, commands::add ), removedProperties,
                            convertRelationships( createdRelationships, commands::add ), convertRelationships( deletedRelationships, commands::add ) ) );
        }

        private IntObjectMap<DenseRelationships> convertRelationships( MutableIntObjectMap<MutableNodeRecordData.Relationships> relationships,
                Consumer<StorageCommand> commandConsumer )
        {
            MutableIntObjectMap<DenseRelationships> converted = IntObjectMaps.mutable.empty();
            relationships.forEachKeyValue( ( type, value ) ->
            {
                DenseRelationships convertedRelationships = new DenseRelationships( type );
                converted.put( type, convertedRelationships );
                value.relationships.forEach( relationship -> convertedRelationships.add( relationship.internalId,
                        relationship.otherNode, relationship.outgoing, convertProperties( relationship.properties, commandConsumer ) ) );
            } );
            return converted;
        }

        private IntObjectMap<ByteBuffer> convertProperties( IntObjectMap<Value> properties, Consumer<StorageCommand> commandConsumer )
        {
            MutableIntObjectMap<ByteBuffer> converted = IntObjectMaps.mutable.empty();
            properties.forEachKeyValue( ( key, value ) -> converted.put( key, serializeValue( bigValueStore, value, commandConsumer ) ) );
            return converted;
        }

        void movePropertiesAndRelationshipsFrom( MutableNodeRecordData data )
        {
            addedProperties.putAll( data.properties );
            createdRelationships.putAll( data.relationships );
            data.clearPropertiesAndRelationships();
        }
    }

    private static ByteBuffer serializeValue( SimpleBigValueStore bigValueStore, Value value, Consumer<StorageCommand> commandConsumer )
    {
        // TODO hand-wavy upper limit
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigValueStore, commandConsumer, buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }
}
