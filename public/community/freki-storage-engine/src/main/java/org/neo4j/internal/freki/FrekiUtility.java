package org.neo4j.internal.freki;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationships;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeData.*;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.internal.freki.StreamVByte.SINGLE_VLONG_MAX_SIZE;

public class FrekiUtility {
    static final int PROPERTIES = Header.OFFSET_PROPERTIES;
    static final int RELATIONSHIPS = Header.OFFSET_RELATIONSHIPS;
    static final int DEGREES = Header.OFFSET_DEGREES;
    static final int RELATIONSHIPS_OFFSETS = Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS;
    static final int NEXT_INTERNAL_RELATIONSHIP_ID = Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID;
    static final int RECORD_POINTER = Header.OFFSET_RECORD_POINTER;
    static final int LABELS = Header.NUM_OFFSETS;

   /* void createRelationships(long relId, int type, long startNode, long endNode, Iterable<StorageProperty> addedProperties)
    {
        Collection<StorageCommand> bigValueCommands = new ArrayList<>();
        Consumer<StorageCommand> bigValueCommandConsumer = bigValueCommands::add;
        long internalRelId = relId >>> 40;
        NodeUpdates updatesSource = new NodeUpdates( startNode, stores, bigValueCommandConsumer, cursorTracer );
        //get the sparse or dense node for the
        SparseRecordAndData sparse = new SparseRecordAndData( startNode, stores, cursorTracer );
        Relationship relationship = new Relationship( id, sourceNode, otherNode, type, outgoing );



    }

    private abstract static class NodeDataModifier
    {
        abstract void updateLabels( LongSet added, LongSet removed );

        abstract void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed );

        abstract void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties );

        abstract void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing );

        abstract void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing,
                                                    Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed );

        abstract void delete();
    }
    private static class SparseRecordAndData extends NodeDataModifier
    {
        private MutableNodeData data;
        private boolean deleted;
        private long nodeId;

        SparseRecordAndData( long nodeId, MainStores stores, PageCursorTracer cursorTracer )
        {
            this.nodeId = nodeId;
            data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer );
        }

        MutableNodeData add( Record record )
        {
            data.deserialize( record );
            return data;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            added.forEach( label -> data.addLabel( toIntExact( label ) ) );
            removed.forEach( label -> data.removeLabel( toIntExact( label ) ) );
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> properties )
        {
            MutableNodeData.Relationship relationship = data.createRelationship( internalId, targetNode, type, outgoing );
            for ( StorageProperty property : properties )
            {
                relationship.addProperty( property.propertyKeyId(), property.value() );
            }
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            added.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            changed.forEach( p -> data.setNodeProperty( p.propertyKeyId(), p.value() ) );
            removed.forEach( data::removeNodeProperty );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            data.deleteRelationship( internalId, type, otherNode, outgoing );
        }

        @Override
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                                           Iterable<StorageProperty> changed, IntIterable removed )
        {
            data.updateRelationshipProperties( internalId, type, nodeId, otherNode, outgoing, added, changed, removed );
        }

        @Override
        public void delete()
        {
            this.deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            // Sanity-check so that, if this node has been deleted it cannot have any relationships left in it
            if ( deleted )
            {
                if ( data.hasRelationships() )
                {
                    throw new DeletedNodeStillHasRelationships( data.nodeId );
                }
            }
        }
    }

    private static class DenseRecordAndData extends NodeDataModifier
    {
        // meta
        private final SparseRecordAndData sparse;
        private final DenseRelationshipStore store;
        private final SimpleBigValueStore bigValueStore;
        private final Consumer<StorageCommand> bigValueConsumer;
        private final PageCursorTracer cursorTracer;
        private boolean deleted;

        // changes
        // TODO it feels like we've simply moving tx-state data from one form to another and that's probably true and can probably be improved on later
        private final MutableIntObjectMap<DenseRelationships> relationshipUpdates = IntObjectMaps.mutable.empty();

        DenseRecordAndData( SparseRecordAndData sparse, DenseRelationshipStore store, SimpleBigValueStore bigValueStore,
                            Consumer<StorageCommand> bigValueConsumer, PageCursorTracer cursorTracer )
        {
            this.sparse = sparse;
            this.store = store;
            this.bigValueStore = bigValueStore;
            this.bigValueConsumer = bigValueConsumer;
            this.cursorTracer = cursorTracer;
        }

        private long nodeId()
        {
            return sparse.nodeId;
        }

        @Override
        public void updateLabels( LongSet added, LongSet removed )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createRelationship( long internalId, long targetNode, int type, boolean outgoing, Iterable<StorageProperty> addedProperties )
        {
            createRelationship( internalId, targetNode, type, outgoing, serializeAddedProperties( addedProperties, IntObjectMaps.mutable.empty() ) );
        }

        private void createRelationship( long internalId, long targetNode, int type, boolean outgoing, IntObjectMap<PropertyUpdate> properties )
        {
            // For sparse representation the high internal relationship ID counter is simply the highest of the existing relationships,
            // decided when loading the node. But for dense nodes we won't load all relationships and will therefore need to keep
            // this counter in an explicit field in the small record. This call keeps that counter updated.
            sparse.data.registerInternalRelationshipId( internalId );
            sparse.data.addDegree( type, calculateDirection( targetNode, outgoing ), 1 );
            relationshipUpdatesForType( type ).insert( new DenseRelationships.DenseRelationship( internalId, targetNode, outgoing, properties ) );
        }

        private RelationshipDirection calculateDirection( long targetNode, boolean outgoing )
        {
            return nodeId() == targetNode ? RelationshipDirection.LOOP : outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING;
        }

        private DenseRelationships relationshipUpdatesForType( int type )
        {
            return relationshipUpdates.getIfAbsentPutWithKey( type, t -> new DenseRelationships( nodeId(), t ) );
        }

        @Override
        public void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
        {
            // TODO have some way of at least saying whether or not this relationship had properties, so that this loading can be skipped completely
            sparse.data.addDegree( type, calculateDirection( otherNode, outgoing ), -1 );
            relationshipUpdatesForType( type ).delete( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing,
                    store.loadRelationshipProperties( nodeId(), internalId, type, otherNode, outgoing, PropertyUpdate::remove, cursorTracer ) ) );
        }

        @Override
        public void updateNodeProperties( Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void updateRelationshipProperties( long internalId, int type, long otherNode, boolean outgoing, Iterable<StorageProperty> added,
                                           Iterable<StorageProperty> changedIterable, IntIterable removedIterable )
        {
            MutableIntObjectMap<PropertyUpdate> properties =
                    store.loadRelationshipProperties( nodeId(), internalId, type, otherNode, outgoing, PropertyUpdate::add, cursorTracer );
            for ( StorageProperty property : added )
            {
                int key = property.propertyKeyId();
                properties.put( key, PropertyUpdate.add( key, serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) );
            }
            Iterator<StorageProperty> changed = changedIterable.iterator();
            IntIterator removed = removedIterable.intIterator();
            if ( changed.hasNext() || removed.hasNext() )
            {
                while ( changed.hasNext() )
                {
                    StorageProperty property = changed.next();
                    int key = property.propertyKeyId();
                    PropertyUpdate existing = properties.get( key );
                    properties.put( key, PropertyUpdate.change( key, existing.after, serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) );
                }
                while ( removed.hasNext() )
                {
                    int key = removed.next();
                    PropertyUpdate existing = properties.get( key );
                    properties.put( key, PropertyUpdate.remove( key, existing.after ) );
                }
            }
            relationshipUpdatesForType( type ).insert( new DenseRelationships.DenseRelationship( internalId, otherNode, outgoing, properties ) );
        }

        @Override
        public void delete()
        {
            deleted = true;
        }

        void prepareForCommandExtraction() throws ConstraintViolationTransactionFailureException
        {
            if ( deleted )
            {
                // This dense node has now been deleted, verify that all its relationships have also been removed in this transaction
                if ( sparse.data.hasAnyDegrees() )
                {
                    throw new DeletedNodeStillHasRelationships( nodeId() );
                }
            }
        }

        void createCommands( Consumer<StorageCommand> commands )
        {
            commands.accept( new FrekiCommand.DenseNode( nodeId(), relationshipUpdates ) );
        }

        //
         // Moving from a sparse --> dense will have all data look like "created", since the before-state is the before-state of the sparse record
         //that it comes from. This differs from changes to an existing dense node where all changes need to follow the added/changed/removed pattern.
         //
        void moveDataFrom( SparseRecordAndData sparseData )
        {
            // We're moving to the dense store, which from its POV all relationships will be created so therefore
            // start at 0 degrees and all creations will increment those degrees.
            sparseData.data.visitRelationships( ( type, fromRelationships ) -> fromRelationships.relationships.forEach(
                    from -> createRelationship( from.internalId, from.otherNode, from.type, from.outgoing,
                            serializeAddedProperties( from.properties, IntObjectMaps.mutable.empty() ) ) ) );
            MutableNodeData nextRelationshipIdData = sparseData.data;
            nextRelationshipIdData.setNextInternalRelationshipId( nextRelationshipIdData.getNextInternalRelationshipId() );
            MutableNodeData relationshipsData = sparseData.data;
            relationshipsData.clearRelationships();
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( IntObjectMap<Value> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEachKeyValue( ( key, value ) -> target.put( key, add( key, serializeValue( bigValueStore, value, bigValueConsumer ) ) ) );
            return target;
        }

        private IntObjectMap<PropertyUpdate> serializeAddedProperties( Iterable<StorageProperty> properties, MutableIntObjectMap<PropertyUpdate> target )
        {
            properties.forEach( property -> target.put( property.propertyKeyId(),
                    add( property.propertyKeyId(), serializeValue( bigValueStore, property.value(), bigValueConsumer ) ) ) );
            return target;
        }
    }

    private static class RecordChain
    {
        private final Record record;
        private RecordChain next;

        RecordChain( Record record )
        {
            this.record = record;
        }
    }

    public static NodeDataModifier[] load(long nodeId, MainStores stores, RecordChain[] firstAndLastRecords, PageCursorTracer cursorTracer)
    {
        SparseRecordAndData sparse = new SparseRecordAndData( nodeId, stores, cursorTracer );
        Record x1 = readRecord( stores, 0, nodeId, cursorTracer );
        if ( x1 == null )
        {
            throw new IllegalStateException( "Node[" + nodeId + "] should have existed" );
        }

        MutableNodeData data = sparse.add( x1 );
        firstAndLastRecords = new RecordChain[]{new RecordChain( x1 ), new RecordChain( x1 )};
        firstBeforeRecord = lastBeforeRecord = new RecordChain( x1 );
        long fwPointer;
        while ( (fwPointer = data.getLastLoadedForwardPointer()) != NULL )
        {
            Record xL = readRecord( stores, sizeExponentialFromRecordPointer( fwPointer ), idFromRecordPointer( fwPointer ), cursorTracer );
            if ( xL == null )
            {
                throw new IllegalStateException( x1 + " points to " + recordPointerToString( fwPointer ) + " that isn't in use" );
            }
            sparse.add( xL );
            RecordChain chain = new RecordChain( xL );
            chain.next = firstAndLastRecords[0];
            firstAndLastRecords[0] = chain;
        }
        DenseRecordAndData dense = null;
        if ( data.isDense() )
        {
            dense = new GraphUpdates.DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommandConsumer, cursorTracer );
        }
        return new NodeDataModifier[]{sparse, dense};
    }

    public static void serialize(long nodeId, MainStores stores, PageCursorTracer cursorTracer, NodeDataModifier recordData, Record[] firstAndLastRecord,
                   ByteBuffer smallBuffer, ByteBuffer maxBuffer, ByteBuffer[] intermediateBuffers, Consumer<StorageCommand> otherCommands, Header x1Header,
                   Header xLHeader )
            throws ConstraintViolationTransactionFailureException
    {
        Collection<StorageCommand> bigValueCommands = new ArrayList<>();
        NodeDataModifier[] sparseOrDense = load(nodeId, stores, firstAndLastRecord, cursorTracer);

        SparseRecordAndData sparse = (SparseRecordAndData)sparseOrDense[0];
        DenseRecordAndData dense = (DenseRecordAndData)sparseOrDense[1];
        Record x1Before = firstAndLastRecord[0];
        Record xLBefore = firstAndLastRecord[1];

        smallBuffer.clear();
        maxBuffer.clear();
        for ( ByteBuffer buffer : intermediateBuffers )
        {
            buffer.clear();
        }

        boolean isDense = false;
        for ( MutableNodeData data : sparse.datas )
        {
            isDense |= data.serializeMainData( intermediateBuffers, stores.bigPropertyValueStore, bigValueCommands::add );
        }

        intermediateBuffers[LABELS].flip();
        intermediateBuffers[PROPERTIES].flip();
        intermediateBuffers[RELATIONSHIPS].flip();
        intermediateBuffers[RELATIONSHIPS_OFFSETS].flip();
        intermediateBuffers[DEGREES].flip();
        int labelsSize = intermediateBuffers[LABELS].limit();
        int propsSize = intermediateBuffers[PROPERTIES].limit();
        int relsSize = intermediateBuffers[RELATIONSHIPS].limit() + intermediateBuffers[RELATIONSHIPS_OFFSETS].limit();
        int degreesSize = intermediateBuffers[DEGREES].limit();
        x1Header.clearMarks();
        if ( !isDense )
        {
            // Then at least see if the combined parts are larger than x8
            x1Header.mark( Header.FLAG_LABELS, labelsSize > 0 );
            x1Header.mark( Header.OFFSET_PROPERTIES, propsSize > 0 );
            x1Header.mark( Header.OFFSET_RELATIONSHIPS, relsSize > 0 );
            x1Header.mark( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, relsSize > 0 );
            x1Header.mark( Header.OFFSET_RECORD_POINTER, true );
            if ( labelsSize + propsSize + relsSize + x1Header.spaceNeeded() + SINGLE_VLONG_MAX_SIZE > stores.largestMainStore().recordDataSize() )
            {
                // We _flip to dense_ a bit earlier than absolutely optimal, but after that the x8 record can be used for other things
                isDense = true;
            }
        }

        if ( isDense )
        {
            if ( dense == null )
            {
                dense = new DenseRecordAndData( sparse, stores.denseStore, stores.bigPropertyValueStore, bigValueCommands:: add, cursorTracer );
                dense.moveDataFrom( sparse );
                intermediateBuffers[RELATIONSHIPS_OFFSETS].clear().flip();
                intermediateBuffers[RELATIONSHIPS].clear().flip();
                relsSize = 0;

                sparse.dataFor( Header.OFFSET_DEGREES ).serializeDegrees( intermediateBuffers[DEGREES].clear() );
                intermediateBuffers[DEGREES].flip();
                degreesSize = intermediateBuffers[DEGREES].limit();
            }
            dense.createCommands( otherCommands );
        }

        // X LEGO TIME
        int miscSize = 0;
        x1Header.clearMarks();
        x1Header.mark( Header.FLAG_LABELS, labelsSize > 0 );
        x1Header.mark( Header.OFFSET_PROPERTIES, propsSize > 0 );
        int nextInternalRelIdSize = 0;
        if ( isDense )
        {
            x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, true );
            x1Header.mark( Header.OFFSET_DEGREES, true );
            x1Header.mark( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID, true );
            sparse.dataFor( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ).serializeNextInternalRelationshipId(
                    intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID] );
            intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].flip();
            nextInternalRelIdSize = intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID].limit();
            miscSize += nextInternalRelIdSize;
        }
        else if ( relsSize > 0 )
        {
            x1Header.mark( Header.OFFSET_RELATIONSHIPS, true );
            x1Header.mark( Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS, true );
        }

        if ( x1Header.spaceNeeded() + labelsSize + propsSize + Math.max( relsSize, degreesSize ) + miscSize <= stores.mainStore.recordDataSize() )
        {
            //WE FIT IN x1
            serializeParts( smallBuffer, intermediateBuffers, x1Header, null );
            x1Command( nodeId, x1Before, smallBuffer, otherCommands );
            if ( xLBefore != null )
            {
                otherCommands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
            }
            return;
        }

        //we did not fit in x1 only, fit as many things as possible in x1
        int worstCaseMiscSize = miscSize + SINGLE_VLONG_MAX_SIZE;

        x1Header.clearMarks();
        // build x1 header
        x1Header.mark( Header.OFFSET_RECORD_POINTER, true );
        x1Header.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
        int spaceLeftInX1 = stores.mainStore.recordDataSize() - worstCaseMiscSize;
        spaceLeftInX1 = tryKeepInX1( x1Header, labelsSize, spaceLeftInX1, Header.FLAG_LABELS );
        spaceLeftInX1 = tryKeepInX1( x1Header, nextInternalRelIdSize, spaceLeftInX1, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
        spaceLeftInX1 = tryKeepInX1( x1Header, propsSize, spaceLeftInX1, Header.OFFSET_PROPERTIES );
        spaceLeftInX1 = tryKeepInX1( x1Header, degreesSize, spaceLeftInX1, Header.OFFSET_DEGREES );
        tryKeepInX1( x1Header, relsSize, spaceLeftInX1, Header.OFFSET_RELATIONSHIPS, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );

        // build xL header and serialize
        xLHeader.clearMarks();
        xLHeader.mark( Header.FLAG_HAS_DENSE_RELATIONSHIPS, isDense );
        prepareRecordPointer( xLHeader, intermediateBuffers[RECORD_POINTER], buildRecordPointer( 0, nodeId ) );
        movePartToXL( x1Header, xLHeader, labelsSize, Header.FLAG_LABELS );
        movePartToXL( x1Header, xLHeader, propsSize, Header.OFFSET_PROPERTIES );
        movePartToXL( x1Header, xLHeader, degreesSize, Header.OFFSET_DEGREES );
        movePartToXL( x1Header, xLHeader, nextInternalRelIdSize, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
        movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS );
        movePartToXL( x1Header, xLHeader, relsSize, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
        serializeParts( maxBuffer, intermediateBuffers, xLHeader, x1Header );
        SimpleStore xLStore = stores.storeSuitableForRecordSize( maxBuffer.limit(), 1 );
        long forwardPointer = xLargeCommands( nodeId, x1Before, xLBefore, maxBuffer, xLStore, otherCommands, cursorTracer );

        // serialize x1
        prepareRecordPointer( x1Header, intermediateBuffers[RECORD_POINTER], forwardPointer );
        serializeParts( smallBuffer, intermediateBuffers, x1Header, xLHeader );
        x1Command( nodeId, x1Before, smallBuffer, otherCommands );
    }

    private static void serializeParts( ByteBuffer into, ByteBuffer[] intermediateBuffers, Header header, Header referenceHeader )
    {
        header.allocateSpace( into );
        if ( header.hasMark( Header.FLAG_LABELS ) )
        {
            into.put( intermediateBuffers[LABELS] );
        }
        serializePart( into, intermediateBuffers[PROPERTIES], header, Header.OFFSET_PROPERTIES );
        serializePart( into, intermediateBuffers[RELATIONSHIPS], header, Header.OFFSET_RELATIONSHIPS );
        serializePart( into, intermediateBuffers[RELATIONSHIPS_OFFSETS], header, Header.OFFSET_RELATIONSHIPS_TYPE_OFFSETS );
        serializePart( into, intermediateBuffers[DEGREES], header, Header.OFFSET_DEGREES );
        serializePart( into, intermediateBuffers[RECORD_POINTER], header, Header.OFFSET_RECORD_POINTER );
        serializePart( into, intermediateBuffers[NEXT_INTERNAL_RELATIONSHIP_ID], header, Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID );
        int endPosition = into.position();
        header.serialize( into.position( 0 ), referenceHeader );
        into.position( endPosition ).flip();
    }

    private static void serializePart( ByteBuffer into, ByteBuffer part, Header header, int slot )
    {
        if ( header.hasMark( slot ) )
        {
            header.setOffset( slot, into.position() );
            into.put( part );
        }
    }
    private static void x1Command( long nodeId, Record x1Before, ByteBuffer smallBuffer, Consumer<StorageCommand> commands )
    {
        Record before = x1Before != null ? x1Before : new Record( 0, nodeId );
        Record after = recordForData( nodeId, smallBuffer, 0 );
        commands.accept( new FrekiCommand.SparseNode( nodeId, before, after ) );
    }

    private static Record deletedVersionOf( Record record )
    {
        Record deletedRecord = new Record( record.sizeExp(), record.id );
        deletedRecord.setFlag( FLAG_IN_USE, false );
        return deletedRecord;
    }

    private static void prepareRecordPointer( Header header, ByteBuffer intermediateBuffer, long recordPointer )
    {
        intermediateBuffer.clear();
        header.mark( Header.OFFSET_RECORD_POINTER, true );
        serializeRecordPointer( intermediateBuffer, recordPointer );
        intermediateBuffer.flip();
    }

    private static void movePartToXL( Header header, Header xlHeader, int partSize, int offset )
    {
        xlHeader.mark( offset, partSize > 0 && !header.hasMark( offset ) );
    }

    private static int tryKeepInX1( Header header, int partSize, int spaceLeftInX1, int... slots )
    {
        if ( partSize > 0 )
        {
            for ( int slot : slots )
            {
                header.mark( slot, true );
            }
            if ( partSize <= spaceLeftInX1 - header.spaceNeeded() )
            {
                // ok this part can live in x1
                spaceLeftInX1 -= partSize;
            }
            else
            {
                for ( int slot : slots )
                {
                    header.mark( slot, false );
                }
            }
        }
        return spaceLeftInX1;
    }

    private static long xLargeCommands( long nodeId, Record x1Before, Record xLBefore, ByteBuffer maxBuffer, SimpleStore store, Consumer<StorageCommand> commands, PageCursorTracer cursorTracer )
    {
        Record after;
        int sizeExp = store.recordSizeExponential();
        if ( xLBefore != null && xLBefore.sizeExp() == sizeExp )
        {
            // There was a large record before and we're just modifying it
            commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, after = recordForData( xLBefore.id, maxBuffer, sizeExp ) ) );
        }
        else if ( xLBefore != null && xLBefore.sizeExp() != sizeExp )
        {
            // There was a large record before, but this time it'll be of a different size, so a different one
            commands.accept( new FrekiCommand.SparseNode( nodeId, xLBefore, deletedVersionOf( xLBefore ) ) );
            long recordId = store.nextId( cursorTracer );
            commands.accept(
                    new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
        }
        else
        {
            // There was no large record before at all
            long recordId = store.nextId( cursorTracer );
            commands.accept(
                    new FrekiCommand.SparseNode( nodeId, new Record( sizeExp, recordId ), after = recordForData( recordId, maxBuffer, sizeExp ) ) );
        }

        return buildRecordPointer( after.sizeExp(), after.id );
    }

    private static Record recordForData( long recordId, ByteBuffer buffer, int sizeExp )
    {
        Record after = new Record( sizeExp, recordId );
        after.setFlag( FLAG_IN_USE, true );
        ByteBuffer byteBuffer = after.data();
        byteBuffer.put( buffer.array(), 0, buffer.limit() );
        byteBuffer.position( buffer.limit() ).flip();
        return after;
    }

    private static Record readRecord( MainStores stores, int sizeExp, long id, PageCursorTracer cursorTracer )
    {
        SimpleStore store = stores.mainStore( sizeExp );
        try ( PageCursor cursor = store.openReadCursor( cursorTracer ) )
        {
            if ( store.exists( cursor, id ) )
            {
                Record record = store.newRecord();
                store.read( cursor, record, id );
                return record;
            }
            return null;
        }
    }

    static ByteBuffer serializeValue( SimpleBigValueStore bigValueStore, Value value, Consumer<StorageCommand> bigValueCommandConsumer )
    {
        // TODO hand-wavy upper limit
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigValueStore, bigValueCommandConsumer, buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }

    */
}
