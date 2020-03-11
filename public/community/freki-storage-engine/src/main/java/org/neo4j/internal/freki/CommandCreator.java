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
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
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
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.freki.FrekiCommand.Mode;
import org.neo4j.internal.freki.MutableNodeRecordData.Relationship;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationships;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.idFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.internalRelationshipIdFromRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.isDenseFromForwardPointer;
import static org.neo4j.internal.freki.MutableNodeRecordData.sizeExponentialFromForwardPointer;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

class CommandCreator implements TxStateVisitor
{
    private final Collection<StorageCommand> bigValueCommands = new ArrayList<>();
    private final Consumer<StorageCommand> bigValueCommandConsumer = bigValueCommands::add;
    private final Collection<StorageCommand> commands;
    private final Stores stores;
    private final ConstraintRuleAccessor constraintSemantics;
    private final PageCursorTracer cursorTracer;
    private final MutableLongObjectMap<Mutation> mutations = LongObjectMaps.mutable.empty();

    CommandCreator( Collection<StorageCommand> commands, Stores stores, ConstraintRuleAccessor constraintSemantics, PageCursorTracer cursorTracer )
    {
        this.commands = commands;
        this.stores = stores;
        this.constraintSemantics = constraintSemantics;
        this.cursorTracer = cursorTracer;
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
        long internalRelationshipId = internalRelationshipIdFromRelationshipId( id );
        createRelationship( internalRelationshipId, type, startNode, endNode, true, addedProperties );
        if ( startNode != endNode )
        {
            createRelationship( internalRelationshipId, type, endNode, startNode, false, addedProperties );
        }
    }

    private void createRelationship( long internalRelationshipId, int type, long sourceNode, long targetNode, boolean outgoing,
            Iterable<StorageProperty> addedProperties )
    {
        getOrLoad( sourceNode ).current.createRelationship( internalRelationshipId, targetNode, type, outgoing, addedProperties );
    }

    @Override
    public void visitDeletedRelationship( long id, int type, long startNode, long endNode )
    {
        long internalRelationshipId = internalRelationshipIdFromRelationshipId( id );
        getOrLoad( startNode ).current.deleteRelationship( internalRelationshipId, type, endNode, true );
        if ( startNode != endNode )
        {
            getOrLoad( endNode ).current.deleteRelationship( internalRelationshipId, type, startNode, false );
        }
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
    {
        getOrLoad( id ).current.updateNodeProperties( added, changed, removed );
    }

    @Override
    public void visitRelPropertyChanges( long relationshipId, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
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
        constraint = constraint.withId( stores.schemaStore.nextSchemaRuleId( cursorTracer ) );
        commands.add( new FrekiCommand.Schema( constraint, Mode.CREATE ) );
        switch ( constraint.type() )
        {
        case UNIQUE:
            // This also means updating the index to have this constraint as owner
            IndexDescriptor index = (IndexDescriptor) stores.schemaStore.loadRule( constraint.asUniquenessConstraint().ownedIndexId(), cursorTracer );
            commands.add( new FrekiCommand.Schema(
                    constraintSemantics.createUniquenessConstraintRule( constraint.getId(), constraint.asUniquenessConstraint(), index.getId() ),
                    Mode.UPDATE ) );
            break;
        case UNIQUE_EXISTS:
            IndexDescriptor indexRule = (IndexDescriptor) stores.schemaStore.loadRule( constraint.asNodeKeyConstraint().ownedIndexId(), cursorTracer );
            commands.add( new FrekiCommand.Schema(
                    constraintSemantics.createNodeKeyConstraintRule( constraint.getId(), constraint.asNodeKeyConstraint(), indexRule.getId() ), Mode.UPDATE ) );
            break;
        case EXISTS:
            commands.add( new FrekiCommand.Schema( constraintSemantics.createExistenceConstraint( constraint.getId(), constraint ), Mode.CREATE ) );
            break;
        default:
            throw new UnsupportedOperationException( "Unknown constraint type " + constraint.type() );
        }
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor constraint ) throws KernelException
    {
        constraint = stores.schemaStore.loadRule( constraint, cursorTracer );
        commands.add( new FrekiCommand.Schema( constraint, Mode.DELETE ) );
        if ( constraint.type() == ConstraintType.UNIQUE )
        {
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
    public void close() throws KernelException
    {
        List<StorageCommand> otherCommands = new ArrayList<>();
        for ( Mutation mutation : mutations )
        {
            RecordAndData abandonedLargeRecord = null;
            while ( true )
            {
                try
                {
                    mutation.current.prepareForCommandExtraction();
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
                mutation.small.prepareForCommandExtraction();
            }

            mutation.small.createCommands( otherCommands ); //Small
            if ( abandonedLargeRecord != null ) //Large
            {
                abandonedLargeRecord.createCommands( otherCommands );
            }
            if ( mutation.large != null ) //Large/Dense
            {
                mutation.large.createCommands( otherCommands );
            }
        }

        commands.addAll( bigValueCommands );
        commands.addAll( otherCommands );
    }

    private Mutation createNew( SimpleStore store, long id )
    {
        Mutation mutation = new Mutation();
        mutation.small = new SparseRecordAndData( store, id, null );
        mutation.small.markInUse( true );
        mutation.small.markCreated();
        mutation.current = mutation.small;
        return mutation;
    }

    private Mutation getOrLoad( long nodeId )
    {
        Mutation mutation = mutations.get( nodeId );
        if ( mutation == null )
        {
            mutation = new Mutation();
            mutation.small = new SparseRecordAndData( stores.mainStore, nodeId, null );
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
                    mutation.large = new SparseRecordAndData( largeStore, fwId, mutation.small );
                }
                else
                {
                    mutation.large = new DenseRecordAndData( mutation.small );
                }
                mutation.current = mutation.large;
                mutation.current.loadExistingData();
            }
            mutations.put( nodeId, mutation );
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
            if ( small != current )
            {
                current.markInUse( false );
            }
        }
    }

    private abstract static class RecordAndData
    {
        abstract void markCreated();

        abstract boolean isCreated();

        abstract void markInUse( boolean inUse );

        abstract void loadExistingData();

        abstract void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties );

        abstract void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed );

        abstract void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing );

        abstract void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException;

        abstract RecordAndData growAndRelocate();

        abstract long asForwardPointer();

        abstract void createCommands( Collection<StorageCommand> commands );
    }

    private class SparseRecordAndData extends RecordAndData
    {
        private final SimpleStore store;
        private final SparseRecordAndData smallRecord;

        private boolean created;
        private Record before;
        private Record after;
        // data here is really accidental state, a deserialized objectified version of the data found in the byte[] of the "after" record
        private MutableNodeRecordData data;

        SparseRecordAndData( SimpleStore store, long id, SparseRecordAndData smallRecord )
        {
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
        void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
            Relationship relationship = data.createRelationship( internalId, targetNode, type, outgoing );
            for ( StorageProperty property : properties )
            {
                relationship.addProperty( property.propertyKeyId(), property.value() );
            }
        }

        @Override
        void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            added.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            changed.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            removed.forEach( p -> data.removeNodeProperty( p ) );
        }

        void addLabels( LongSet added )
        {
            added.forEach( label -> data.labels.add( toIntExact( label ) ) );
        }

        void removeLabels( LongSet removed )
        {
            removed.forEach( label -> data.labels.remove( toIntExact( label ) ) );
        }

        @Override
        void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            data.deleteRelationship( internalId, type, otherNode, outgoing );
        }

        long getForwardPointer()
        {
            return data.getForwardPointer();
        }

        @Override
        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Sanity-check so that, if this node has been deleted it cannot have any relationships left in it
            if ( !after.hasFlag( FLAG_IN_USE ) && after.sizeExp() == 0 && data.hasRelationships() )
            {
                throw new DeletedNodeStillHasRelationships( after.id );
            }

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
                SparseRecordAndData largerRecord = new SparseRecordAndData( largerStore, -1, smallRecord );
                data.movePropertiesAndRelationshipsTo( largerRecord.data );
                result = largerRecord;
            }
            else
            {
                // Time to move over to GBPTree-style data
                DenseRecordAndData denseRecord = new DenseRecordAndData( smallRecord );
                denseRecord.movePropertiesAndRelationshipsFrom( data );
                result = denseRecord;
            }
            return result;
        }

        private void acquireId()
        {
            long id = store.nextId( cursorTracer );
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

    private class DenseRecordAndData extends RecordAndData
    {
        // meta
        private final SparseRecordAndData smallRecord;
        private boolean created;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private MutableIntObjectMap<PropertyUpdate> propertyUpdates = IntObjectMaps.mutable.empty();
        private MutableIntObjectMap<DenseRelationships> relationshipUpdates = IntObjectMaps.mutable.empty();

        DenseRecordAndData( SparseRecordAndData smallRecord )
        {
            this.smallRecord = smallRecord;
        }

        private long nodeId()
        {
            return smallRecord.data.id;
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
            if ( !inUse )
            {
                stores.denseStore.loadAndRemoveNodeProperties( nodeId(), propertyUpdates );
            }
        }

        @Override
        void loadExistingData()
        {
            // Thoughts: we shouldn't really load anything here since we don't need the existing data in order to make changes
            markInUse( true );
        }

        @Override
        void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> addedProperties )
        {
            // For sparse representation the high internal relationship ID counter is simply the highest of the existing relationships,
            // decided when loading the node. But for dense nodes we won't load all relationships and will therefore need to keep
            // this counter in an explicit field in the small record. This call keeps that counter updated.
            smallRecord.data.registerInternalRelationshipId( internalId );
            relationshipUpdatesForType( type ).create( new DenseRelationships.DenseRelationship( internalId, targetNode, outgoing,
                    serializeAddedProperties( addedProperties, IntObjectMaps.mutable.empty() ) ) );
        }

        private DenseRelationships relationshipUpdatesForType( int type )
        {
            return relationshipUpdates.getIfAbsentPutWithKey( type, t ->
            {
                EagerDegrees degrees = stores.denseStore.getDegrees( nodeId(), selection( t, Direction.BOTH ), PageCursorTracer.NULL );
                return new DenseRelationships( nodeId(), t,
                        degrees.rawOutgoingDegree( t ), degrees.rawIncomingDegree( t ), degrees.rawLoopDegree( t ) );
            } );
        }

        @Override
        void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            // TODO have some way of at least saying whether or not this relationship had properties, so that this loading can be skipped completely
            relationshipUpdatesForType( type ).delete( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing,
                    stores.denseStore.loadRelationshipPropertiesForRemoval( nodeId(), internalId, type, otherNode, outgoing ) ) );
        }

        @Override
        void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            stores.denseStore.prepareUpdateNodeProperties( nodeId(), added, changed, removed, bigValueCommandConsumer, propertyUpdates );
        }

        @Override
        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            if ( !smallRecord.after.hasFlag( FLAG_IN_USE ) )
            {
                // This dense node has now been deleted, verify that all its relationships have also been removed in this transaction
                // This is difficult to do efficiently, but we can at least compare degrees and see that an equal amount of deleted relationships
                // exist in this transaction for this node.
                if ( !allRelationshipsHaveBeenDeleted() )
                {
                    throw new DeletedNodeStillHasRelationships( nodeId() );
                }
            }
        }

        private boolean allRelationshipsHaveBeenDeleted()
        {
            EagerDegrees degrees = stores.denseStore.getDegrees( nodeId(), ALL_RELATIONSHIPS, cursorTracer );
            for ( int type : degrees.types() )
            {
                DenseRelationships relationshipsOfType = relationshipUpdates.get( type );
                if ( relationshipsOfType == null || relationshipsOfType.deleted.size() != degrees.totalDegree( type ) )
                {
                    return false;
                }
            }
            return true;
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
            commands.add( new FrekiCommand.DenseNode( nodeId(), propertyUpdates, relationshipUpdates ) );
        }

        /**
         * Moving from a sparse --> dense will have all data look like "created", since the before-state is the before-state of the sparse record
         * that it comes from. This differs from changes to an existing dense node where all changes need to follow the added/changed/removed pattern.
         */
        void movePropertiesAndRelationshipsFrom( MutableNodeRecordData data )
        {
            serializeAddedProperties( data.properties, propertyUpdates );
            data.relationships.forEachKeyValue( ( type, fromRelationships ) ->
            {
                // We're moving to the dense store, which from its POV all relationships will be created so therefore
                // start at 0 degrees and all creations will increment those degrees.
                DenseRelationships relationships = relationshipUpdates.getIfAbsentPut( type, new DenseRelationships( nodeId(), type, 0, 0, 0 ) );
                fromRelationships.relationships.forEach( fromRelationship -> relationships.create(
                        new DenseRelationships.DenseRelationship( fromRelationship.internalId, fromRelationship.otherNode, fromRelationship.outgoing,
                                serializeAddedProperties( fromRelationship.properties, IntObjectMaps.mutable.empty() ) ) ) );
            } );
            smallRecord.data.nextInternalRelationshipId = data.nextInternalRelationshipId;
            data.clearPropertiesAndRelationships();
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( IntObjectMap<Value> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEachKeyValue( ( key, value ) -> target.put( key, add( key, serializeValue( value ) ) ) );
            return target;
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( Iterable<StorageProperty> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEach( property -> target.put( property.propertyKeyId(), add( property.propertyKeyId(), serializeValue( property.value() ) ) ) );
            return target;
        }

        ByteBuffer serializeValue( Value value )
        {
            return CommandCreator.serializeValue( stores.bigPropertyValueStore, value, bigValueCommandConsumer );
        }
    }

    static ByteBuffer serializeValue( SimpleBigValueStore bigValueStore, Value value, Consumer<StorageCommand> commandConsumer )
    {
        // TODO hand-wavy upper limit
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigValueStore, commandConsumer, buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }
}
