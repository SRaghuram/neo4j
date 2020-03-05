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
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.graphdb.Direction;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.freki.CommandCreator.serializeValue;
import static org.neo4j.internal.freki.MutableNodeRecordData.externalRelationshipId;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.freki.PropertyUpdate.change;
import static org.neo4j.internal.freki.PropertyUpdate.remove;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

/**
 * Last resort for dense nodes, can contain any number of entries for a node. Although only properties and relationships (and relationship properties)
 * are stored here, not labels.
 *
 * OBSERVE For the time being this tree is per database, for simplicity. But in the end we'd really want to have one tree per node,
 * i.e. completely node-centric trees.
 */
class DenseStore extends LifecycleAdapter implements Closeable
{
    private static final Comparator<StorageProperty> PROPERTY_SORTER = ( p1, p2 ) -> Integer.compare( p1.propertyKeyId(), p2.propertyKeyId() );
    private static final int MAX_ENTRY_VALUE_SIZE = 256;

    private final DenseStoreLayout layout;
    private final GBPTree<DenseStoreKey,DenseStoreValue> tree;
    private final SimpleBigValueStore bigPropertyValueStore;

    DenseStore( PageCache pageCache, File file, RecoveryCleanupWorkCollector collector, boolean readOnly, PageCacheTracer tracer,
            SimpleBigValueStore bigPropertyValueStore )
    {
        this.bigPropertyValueStore = bigPropertyValueStore;
        this.layout = new DenseStoreLayout();
        this.tree = new GBPTree<>( pageCache, file, layout, 0, GBPTree.NO_MONITOR, GBPTree.NO_HEADER_READER, GBPTree.NO_HEADER_WRITER, collector, readOnly,
                tracer, Sets.immutable.empty() );
    }

    PrefetchingResourceIterator<StorageProperty> getProperties( long nodeId, PageCursorTracer cursorTracer )
    {
        DenseStoreKey from = layout.newKey().initializeProperty( nodeId, Integer.MIN_VALUE );
        DenseStoreKey to = layout.newKey().initializeProperty( nodeId, Integer.MAX_VALUE );
        try
        {
            return new PropertyIterator( tree.seek( from, to, cursorTracer ) )
            {
                @Override
                public int propertyKeyId()
                {
                    return seek.key().tokenId;
                }

                @Override
                public Value value()
                {
                    return PropertyValueFormat.readEagerly( seek.value().data, bigPropertyValueStore );
                }
            };
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Used to efficiently gather a set of node property changes, where existing values are loaded for changed and removed properties.
     */
    void prepareUpdateNodeProperties( long nodeId, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed,
            Consumer<StorageCommand> commandConsumer, MutableIntObjectMap<PropertyUpdate> updates )
    {
        try
        {
            MutableIntSet propertyKeysToLoad = IntSets.mutable.empty();
            changed.forEach( p -> propertyKeysToLoad.add( p.propertyKeyId() ) );
            removed.forEach( propertyKeysToLoad::add );
            MutableIntObjectMap<ByteBuffer> loadedPropertyValues = IntObjectMaps.mutable.withInitialCapacity( propertyKeysToLoad.size() );
            if ( !propertyKeysToLoad.isEmpty() )
            {
                loadRawNodeProperties( nodeId, ( key, value ) ->
                {
                    if ( propertyKeysToLoad.contains( key.tokenId ) )
                    {
                        loadedPropertyValues.put( key.tokenId, value.dataCopy() );
                    }
                } );
            }

            // added - simple, just add them
            for ( StorageProperty property : added )
            {
                updates.put( property.propertyKeyId(),
                        add( property.propertyKeyId(), serializeValue( bigPropertyValueStore, property.value(), commandConsumer ) ) );
            }
            // updates - load and add before/after
            for ( StorageProperty property : changed )
            {
                updates.put( property.propertyKeyId(), change( property.propertyKeyId(), loadedPropertyValues.get( property.propertyKeyId() ),
                        serializeValue( bigPropertyValueStore, property.value(), commandConsumer ) ) );
            }
            // removals - load and add before
            removed.forEach( key -> updates.put( key, remove( key, loadedPropertyValues.get( key ) ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    void loadAndRemoveNodeProperties( long nodeId, MutableIntObjectMap<PropertyUpdate> updates )
    {
        try
        {
            loadRawNodeProperties( nodeId, ( key, value ) -> updates.put( key.tokenId, PropertyUpdate.remove( key.tokenId, value.dataCopy() ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void loadRawNodeProperties( long nodeId, BiConsumer<DenseStoreKey,DenseStoreValue> consumer ) throws IOException
    {
        try ( Seeker<DenseStoreKey,DenseStoreValue> seek = tree.seek( new DenseStoreKey().initializeProperty( nodeId, Integer.MIN_VALUE ),
                new DenseStoreKey().initializeProperty( nodeId, Integer.MAX_VALUE ), PageCursorTracer.NULL ) )
        {
            while ( seek.next() )
            {
                consumer.accept( seek.key(), seek.value() );
            }
        }
    }

    MutableIntObjectMap<PropertyUpdate> loadRelationshipPropertiesForRemoval( long nodeId, long internalId, int type, long otherNodeId, boolean outgoing )
    {
        try
        {
            DenseStoreKey relationship =
                    new DenseStoreKey().initializeRelationship( nodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, otherNodeId, internalId );
            try ( Seeker<DenseStoreKey, DenseStoreValue> seek = tree.seek( relationship, relationship, PageCursorTracer.NULL ) )
            {
                if ( !seek.next() )
                {
                    throw new IllegalStateException( "Relationship about to be removed didn't exist id:" +
                            externalRelationshipId( nodeId, internalId, otherNodeId, outgoing ) );
                }
                MutableIntObjectMap<PropertyUpdate> properties = IntObjectMaps.mutable.empty();
                RelationshipPropertyIterator relationshipProperties = relationshipPropertiesIterator( seek.value().data );
                while ( relationshipProperties.hasNext() )
                {
                    relationshipProperties.next();
                    int key = relationshipProperties.propertyKeyId();
                    properties.put( key, PropertyUpdate.remove( key, relationshipProperties.serializedValue() ) );
                }
                return properties;
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    PrefetchingResourceIterator<RelationshipData> getRelationships( long nodeId, int type, Direction direction, PageCursorTracer cursorTracer )
    {
        // If type is defined (i.e. NOT -1) then we can seek by direction too, otherwise we'll have to filter on direction
        DenseStoreKey from = layout.newKey().initializeRelationship( nodeId,
                type == ANY_RELATIONSHIP_TYPE ? Integer.MIN_VALUE : type, direction, Long.MIN_VALUE, Long.MIN_VALUE );
        DenseStoreKey to = layout.newKey().initializeRelationship( nodeId,
                type == ANY_RELATIONSHIP_TYPE ? Integer.MAX_VALUE : type, direction, Long.MAX_VALUE, Long.MAX_VALUE );
        Predicate<RelationshipData> filter = null;
        if ( type == ANY_RELATIONSHIP_TYPE && direction != Direction.BOTH )
        {
            // TODO A case where we need to filter... not very nice, let's fix this somehow
            filter = rel -> RelationshipSelection.matchesDirection( rel.direction(), direction );
        }

        try
        {
            return new RelationshipIterator( tree.seek( from, to, cursorTracer ), filter )
            {
                @Override
                public long internalId()
                {
                    return key.internalRelationshipId;
                }

                @Override
                public long originNodeId()
                {
                    return key.nodeId;
                }

                @Override
                public long neighbourNodeId()
                {
                    return key.neighbourNodeId;
                }

                @Override
                public int type()
                {
                    return key.tokenId;
                }

                @Override
                public RelationshipDirection direction()
                {
                    return key.direction == Direction.OUTGOING
                           ? originNodeId() == neighbourNodeId() ? LOOP : RelationshipDirection.OUTGOING
                           : RelationshipDirection.INCOMING;
                }

                @Override
                public Iterator<StorageProperty> properties()
                {
                    return relationshipPropertiesIterator( value.data );
                }
            };
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private RelationshipPropertyIterator relationshipPropertiesIterator( ByteBuffer relationshipData )
    {
        int[] propertyKeys = StreamVByte.readIntDeltas( new StreamVByte.IntArrayTarget(), relationshipData ).array();
        return new RelationshipPropertyIterator()
        {
            private int current = -1;
            private Value currentValue;

            @Override
            protected StorageProperty fetchNextOrNull()
            {
                if ( current + 1 >= propertyKeys.length )
                {
                    return null;
                }
                current++;
                if ( currentValue == null && current > 0 )
                {
                    // If we didn't read the value of the previous key then skip it in the buffer
                    relationshipData.position( relationshipData.position() + calculatePropertyValueSizeIncludingTypeHeader( relationshipData ) );
                }
                currentValue = null;
                return this;
            }

            @Override
            public int propertyKeyId()
            {
                return propertyKeys[current];
            }

            @Override
            public Value value()
            {
                if ( currentValue == null )
                {
                    currentValue = PropertyValueFormat.readEagerly( relationshipData, bigPropertyValueStore );
                }
                return currentValue;
            }

            @Override
            ByteBuffer serializedValue()
            {
                assert currentValue == null;
                int from = relationshipData.position();
                int length = calculatePropertyValueSizeIncludingTypeHeader( relationshipData );
                return ByteBuffer.wrap( Arrays.copyOfRange( relationshipData.array(), from, from + length ) );
            }

            @Override
            public boolean isDefined()
            {
                return true;
            }
        };

    }    EagerDegrees getDegrees( long nodeId, RelationshipSelection selection, PageCursorTracer cursorTracer )
    {
        try
        {
            EagerDegrees degrees = new EagerDegrees();
            if ( selection.numberOfCriteria() == 1 )
            {
                RelationshipSelection.Criterion criterion = selection.criterion( 0 );
                int type = criterion.type();
                if ( type != ANY_RELATIONSHIP_TYPE )
                {
                    // OK this is for a single type, we can do a very precise seek
                    DenseStoreKey key = layout.newKey().initializeDegree( nodeId, type );
                    try ( Seeker<DenseStoreKey,DenseStoreValue> seek = tree.seek( key, key, cursorTracer ) )
                    {
                        if ( seek.next() )
                        {
                            seek.value().addTo( type, degrees );
                            return degrees;
                        }
                    }
                }
            }

            // Let's do a range scan over all degrees and then filter instead
            DenseStoreKey from = layout.newKey().initializeDegree( nodeId, Integer.MIN_VALUE );
            DenseStoreKey to = layout.newKey().initializeDegree( nodeId, Integer.MAX_VALUE );
            try ( Seeker<DenseStoreKey,DenseStoreValue> seek = tree.seek( from, to, cursorTracer ) )
            {
                while ( seek.next() )
                {
                    int type = seek.key().tokenId;
                    if ( selection.test( type ) )
                    {
                        seek.value().addTo( type, degrees );
                    }
                }
            }
            return degrees;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    Updater newUpdater( PageCursorTracer cursorTracer ) throws IOException
    {
        Writer<DenseStoreKey,DenseStoreValue> writer = tree.writer( cursorTracer );
        return new Updater()
        {
            private final DenseStoreKey key = new DenseStoreKey();
            private final DenseStoreValue value = new DenseStoreValue();

            @Override
            public void setProperty( long nodeId, int propertyKey, ByteBuffer propertyValue )
            {
                key.initializeProperty( nodeId, propertyKey );
                value.initializeProperty( propertyValue );
                writer.put( key, value );
            }

            @Override
            public void removeProperty( long nodeId, int propertyKey )
            {
                key.initializeProperty( nodeId, propertyKey );
                writer.remove( key );
            }

            @Override
            public void createRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing,
                    IntObjectMap<PropertyUpdate> properties, Function<PropertyUpdate,ByteBuffer> version )
            {
                key.initializeRelationship( sourceNodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, targetNodeId, internalId );
                value.initializeRelationship( properties, version );
                writer.put( key, value );
            }

            @Override
            public void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing )
            {
                key.initializeRelationship( sourceNodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, targetNodeId, internalId );
                writer.remove( key );
            }

            @Override
            public void setDegree( long nodeId, int relationshipType, int outgoing, int incoming, int loop )
            {
                key.initializeDegree( nodeId, relationshipType );
                if ( outgoing == 0 && incoming == 0 && loop == 0 )
                {
                    writer.remove( key );
                }
                else
                {
                    value.initializeDegree( outgoing, incoming, loop );
                    writer.put( key, value );
                }
            }

            @Override
            public void close() throws IOException
            {
                writer.close();
            }
        };
    }

    void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
        tree.checkpoint( ioLimiter, cursorTracer );
    }

    @Override
    public void shutdown() throws IOException
    {
        close();
    }

    @Override
    public void close() throws IOException
    {
        tree.close();
    }

    interface Updater extends Closeable
    {
        void setProperty( long nodeId, int key, ByteBuffer value );

        void removeProperty( long nodeId, int key );

        void createRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> properties,
                Function<PropertyUpdate,ByteBuffer> version );

        void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing );

        /**
         * Setting all-zero degrees will remove the entry.
         */
        void setDegree( long nodeId, int relationshipType, int outgoing, int incoming, int loop );
    }

    private class DenseStoreLayout extends Layout.Adapter<DenseStoreKey,DenseStoreValue>
    {
        DenseStoreLayout()
        {
            super( false, 989898, 0, 1 );
        }

        @Override
        public DenseStoreKey newKey()
        {
            return new DenseStoreKey();
        }

        @Override
        public DenseStoreKey copyKey( DenseStoreKey key, DenseStoreKey into )
        {
            into.nodeId = key.nodeId;
            into.itemType = key.itemType;
            into.tokenId = key.tokenId;
            into.direction = key.direction;
            into.internalRelationshipId = key.internalRelationshipId;
            into.neighbourNodeId = key.neighbourNodeId;
            return into;
        }

        @Override
        public DenseStoreValue newValue()
        {
            return new DenseStoreValue();
        }

        @Override
        public int keySize( DenseStoreKey key )
        {
            switch ( key.itemType )
            {
            case DenseStoreKey.TYPE_DEGREE:
            case DenseStoreKey.TYPE_PROPERTY:
                return 8;
            case DenseStoreKey.TYPE_RELATIONSHIP:
                return 16;
            default:
                throw new IllegalArgumentException( "Unknown item type " + key.itemType );
            }
        }

        @Override
        public int valueSize( DenseStoreValue value )
        {
            return value.data.limit();
        }

        @Override
        public void writeKey( PageCursor cursor, DenseStoreKey key )
        {
            switch ( key.itemType )
            {
            case DenseStoreKey.TYPE_DEGREE:
            case DenseStoreKey.TYPE_PROPERTY:
            {
                // [ sss,iiii][tttt,tttt][tttt,tttt][tttt,tttt]
                // t: tokenId
                // i: item type
                // s: source node size
                // TODO but for now just store the sourceNodeId as an int
                cursor.putInt( (int) key.nodeId );
                int serialized = key.tokenId | (key.itemType << 24);
                cursor.putInt( serialized );
                break;
            }
            case DenseStoreKey.TYPE_RELATIONSHIP:
            {
                // [ sss,iiii][tttt,tttt][tttt,tttt][tttt,tttt]
                // t: tokenId
                // i: item type
                // s: source node size
                // TODO but for now just store the sourceNodeId as an int
                cursor.putInt( (int) key.nodeId );
                int serialized = key.tokenId | (key.itemType << 24) | (key.isOutgoing() ? 0x80000000 : 0);
                cursor.putInt( serialized );
                cursor.putInt( (int) key.internalRelationshipId );
                cursor.putInt( (int) key.neighbourNodeId );
                break;
            }
            default:
                throw new IllegalArgumentException( "Unknown item type " + key.itemType );
            }
        }

        @Override
        public void readKey( PageCursor cursor, DenseStoreKey into, int keySize )
        {
            into.nodeId = cursor.getInt() & 0xFFFFFFFFL;
            int serialized = cursor.getInt();
            into.itemType = (byte) ((serialized >>> 24) & 0xF);
            into.tokenId = serialized & 0xFFFFFF;
            switch ( into.itemType )
            {
            case DenseStoreKey.TYPE_DEGREE:
            case DenseStoreKey.TYPE_PROPERTY:
            {
                // Nothing additional to do
                break;
            }
            case DenseStoreKey.TYPE_RELATIONSHIP:
            {
                boolean outgoing = (serialized & 0x80000000) != 0;
                into.direction = outgoing ? Direction.OUTGOING : Direction.INCOMING;
                into.internalRelationshipId = cursor.getInt() & 0xFFFFFFFFL;
                into.neighbourNodeId = cursor.getInt() & 0xFFFFFFFFL;
                break;
            }
            default:
                cursor.setCursorException( "Unknown item type " + into.itemType );
                break;
            }
        }

        @Override
        public void writeValue( PageCursor cursor, DenseStoreValue value )
        {
            cursor.putBytes( value.data.array(), 0, value.data.limit() );
        }

        @Override
        public void readValue( PageCursor cursor, DenseStoreValue into, int valueSize )
        {
            if ( valueSize > MAX_ENTRY_VALUE_SIZE )
            {
                cursor.setCursorException( "Value unreasonably large" );
                return;
            }
            into.data.clear();
            cursor.getBytes( into.data.array(), 0, valueSize );
            into.data.position( valueSize );
            into.data.flip();
        }

        @Override
        public void initializeAsLowest( DenseStoreKey key )
        {
            key.nodeId = Long.MIN_VALUE;
        }

        @Override
        public void initializeAsHighest( DenseStoreKey key )
        {
            key.nodeId = Long.MAX_VALUE;
        }

        @Override
        public int compare( DenseStoreKey o1, DenseStoreKey o2 )
        {
            int nodeComparison = Long.compare( o1.nodeId, o2.nodeId );
            if ( nodeComparison != 0 )
            {
                return nodeComparison;
            }
            int itemTypeComparison = Byte.compare( o1.itemType, o2.itemType );
            if ( itemTypeComparison != 0 )
            {
                return itemTypeComparison;
            }
            int tokenIdComparison = Integer.compare( o1.tokenId, o2.tokenId );
            if ( tokenIdComparison != 0 )
            {
                return tokenIdComparison;
            }

            byte itemType = o1.itemType; // o1 and o2 both have the same itemType here
            switch ( itemType )
            {
            case DenseStoreKey.TYPE_DEGREE:
            case DenseStoreKey.TYPE_PROPERTY:
                return 0; // nothing more to compare
            case DenseStoreKey.TYPE_RELATIONSHIP:
                // compare direction, if both wants that
                if ( o1.direction != Direction.BOTH && o2.direction != Direction.BOTH )
                {
                    int directionComparison = o1.direction.compareTo( o2.direction );
                    if ( directionComparison != 0 )
                    {
                        return directionComparison;
                    }
                }
                int neighbourComparison = Long.compare( o1.neighbourNodeId, o2.neighbourNodeId );
                if ( neighbourComparison != 0 )
                {
                    return neighbourComparison;
                }
                return Long.compare( o1.internalRelationshipId, o2.internalRelationshipId );
            default:
                throw new IllegalArgumentException( "Unknown item type " + itemType );
            }
        }
    }

    private static class DenseStoreKey
    {
        private static final byte TYPE_DEGREE = 0;
        private static final byte TYPE_PROPERTY = 1;
        private static final byte TYPE_RELATIONSHIP = 2;

        // the order of these fields should match the order in which they are compared
        long nodeId; // temporary so long everything is in the same tree
        byte itemType; // like property or relationship
        int tokenId; // both property and relationship will have exactly one token id
        Direction direction;
        long neighbourNodeId;
        long internalRelationshipId;

        DenseStoreKey initializeDegree( long nodeId, int relationshipType )
        {
            this.nodeId = nodeId;
            this.itemType = TYPE_DEGREE;
            this.tokenId = relationshipType;
            return this;
        }

        DenseStoreKey initializeProperty( long nodeId, int propertyKey )
        {
            this.nodeId = nodeId;
            this.itemType = TYPE_PROPERTY;
            this.tokenId = propertyKey;
            return this;
        }

        DenseStoreKey initializeRelationship( long originNodeId, int type, Direction direction, long neighbourNodeId, long internalId )
        {
            this.nodeId = originNodeId;
            this.itemType = TYPE_RELATIONSHIP;
            this.tokenId = type;
            this.direction = direction;
            this.neighbourNodeId = neighbourNodeId;
            this.internalRelationshipId = internalId;
            return this;
        }

        boolean isOutgoing()
        {
            return direction == Direction.OUTGOING;
        }

        @Override
        public String toString()
        {
            switch ( itemType )
            {
            case TYPE_DEGREE:
                return format( "degree:%d", tokenId );
            case TYPE_PROPERTY:
                return format( "nodeId:%d,Property{key:%d}", nodeId, tokenId );
            case TYPE_RELATIONSHIP:
                return format( "nodeId:%d,Relationship{type:%d,%s,%d}", nodeId, tokenId, isOutgoing() ? "OUT" : "IN", neighbourNodeId );
            default:
                throw new UnsupportedOperationException( "Unrecognized type " + itemType );
            }
        }
    }

    private static class DenseStoreValue
    {
        // TODO for simplicity just have this a ByteBuffer so that the other serialize/deserialize stuff can be used in here too
        // TODO let's just make up some upper limit here and the rest will go to big-value store anyway
        ByteBuffer data = ByteBuffer.wrap( new byte[MAX_ENTRY_VALUE_SIZE] );

        ByteBuffer dataCopy()
        {
            return ByteBuffer.wrap( Arrays.copyOf( data.array(), data.limit() ) );
        }

        void initializeDegree( int outgoing, int incoming, int loop )
        {
            data.clear();
            data.putInt( outgoing );
            data.putInt( incoming );
            data.putInt( loop );
            data.flip();
        }

        void initializeProperty( ByteBuffer value )
        {
            data.clear();
            data.put( value );
            data.flip();
        }

        void initializeRelationship( IntObjectMap<PropertyUpdate> properties, Function<PropertyUpdate,ByteBuffer> version )
        {
            data.clear();
            properties.keySet().toSortedArray();
            int[] sortedKeys = properties.keySet().toSortedArray();
            StreamVByte.writeIntDeltas( sortedKeys, data );
            for ( int key : sortedKeys )
            {
                data.put( version.apply( properties.get( key ) ) );
            }
            data.flip();
        }

        void addTo( int type, EagerDegrees into )
        {
            into.add( type, data.getInt(), data.getInt(), data.getInt() );
        }
    }

    private abstract static class CursorIterator<ITEM> extends PrefetchingResourceIterator<ITEM>
    {
        final Seeker<DenseStoreKey,DenseStoreValue> seek;
        private final Predicate<ITEM> filter;
        DenseStoreKey key;
        DenseStoreValue value;

        CursorIterator( Seeker<DenseStoreKey,DenseStoreValue> seek, Predicate<ITEM> filter )
        {
            this.seek = seek;
            this.filter = filter;
        }

        @Override
        protected ITEM fetchNextOrNull()
        {
            try
            {
                while ( seek.next() )
                {
                    key = seek.key();
                    ITEM item = (ITEM) this;
                    if ( filter != null && !filter.test( item ) )
                    {
                        continue;
                    }
                    value = seek.value();
                    return item;
                }
                return null;
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        public void close()
        {
            try
            {
                seek.close();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    private abstract static class PropertyIterator extends CursorIterator<StorageProperty> implements StorageProperty
    {
        PropertyIterator( Seeker<DenseStoreKey,DenseStoreValue> seek )
        {
            super( seek, null );
        }

        @Override
        public boolean isDefined()
        {
            return true;
        }
    }

    interface RelationshipData
    {
        long internalId();

        long originNodeId();

        long neighbourNodeId();

        int type();

        RelationshipDirection direction();

        Iterator<StorageProperty> properties();
    }

    private abstract static class RelationshipIterator extends CursorIterator<RelationshipData> implements RelationshipData
    {
        RelationshipIterator( Seeker<DenseStoreKey,DenseStoreValue> seek, Predicate<RelationshipData> filter )
        {
            super( seek, filter );
        }
    }

    private abstract static class RelationshipPropertyIterator extends PrefetchingIterator<StorageProperty> implements StorageProperty
    {
        abstract ByteBuffer serializedValue();
    }
}
