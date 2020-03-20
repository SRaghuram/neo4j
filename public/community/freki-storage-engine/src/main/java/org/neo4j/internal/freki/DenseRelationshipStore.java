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

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
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
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.freki.MutableNodeRecordData.externalRelationshipId;
import static org.neo4j.internal.freki.PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader;
import static org.neo4j.internal.freki.StreamVByte.calculateLongSizeIndex;
import static org.neo4j.internal.freki.StreamVByte.readLongValue;
import static org.neo4j.internal.freki.StreamVByte.sizeOfLongSizeIndex;
import static org.neo4j.internal.freki.StreamVByte.writeIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.writeLongValue;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

/**
 * Last resort for dense nodes, can contain any number of entries for a node. Although only properties and relationships (and relationship properties)
 * are stored here, not labels.
 *
 * OBSERVE For the time being this tree is per database, for simplicity. But in the end we'd really want to have one tree per node,
 * i.e. completely node-centric trees.
 */
class DenseRelationshipStore extends LifecycleAdapter implements Closeable
{
    private static final int MAX_ENTRY_VALUE_SIZE = 256;

    private final DenseLayout layout;
    private final GBPTree<DenseKey,DenseValue> tree;
    private final SimpleBigValueStore bigPropertyValueStore;

    DenseRelationshipStore( PageCache pageCache, File file, RecoveryCleanupWorkCollector collector, boolean readOnly, PageCacheTracer tracer,
            SimpleBigValueStore bigPropertyValueStore )
    {
        this.bigPropertyValueStore = bigPropertyValueStore;
        this.layout = new DenseLayout();
        this.tree = new GBPTree<>( pageCache, file, layout, 0, GBPTree.NO_MONITOR, GBPTree.NO_HEADER_READER, GBPTree.NO_HEADER_WRITER, collector, readOnly,
                tracer, Sets.immutable.empty() );
    }

    MutableIntObjectMap<PropertyUpdate> loadRelationshipPropertiesForRemoval( long nodeId, long internalId, int type, long otherNodeId, boolean outgoing,
            PageCursorTracer cursorTracer )
    {
        try
        {
            DenseKey relationship =
                    new DenseKey().initialize( nodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, otherNodeId, internalId );
            try ( Seeker<DenseKey,DenseValue> seek = tree.seek( relationship, relationship, cursorTracer ) )
            {
                if ( !seek.next() )
                {
                    throw new IllegalStateException( "Relationship about to be removed didn't exist id:" +
                            externalRelationshipId( nodeId, internalId, otherNodeId, outgoing ) );
                }
                MutableIntObjectMap<PropertyUpdate> properties = IntObjectMaps.mutable.empty();
                RelationshipPropertyIterator relationshipProperties = relationshipPropertiesIterator( seek.value().data, bigPropertyValueStore );
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
        DenseKey from = layout.newKey().initialize( nodeId,
                type == ANY_RELATIONSHIP_TYPE ? Integer.MIN_VALUE : type, direction, Long.MIN_VALUE, Long.MIN_VALUE );
        DenseKey to = layout.newKey().initialize( nodeId,
                type == ANY_RELATIONSHIP_TYPE ? Integer.MAX_VALUE : type, direction, Long.MAX_VALUE, Long.MAX_VALUE );
        Predicate<RelationshipData> filter = null;
        if ( type == ANY_RELATIONSHIP_TYPE && direction != Direction.BOTH )
        {
            // TODO A case where we need to filter... not very nice, let's fix this somehow
            filter = rel -> RelationshipSelection.matchesDirection( rel.direction(), direction );
        }

        try
        {
            return new RelationshipIterator( tree.seek( from, to, cursorTracer ), filter, bigPropertyValueStore );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    RelationshipData getRelationship( long nodeId, int type, Direction direction, long otherNodeId, long internalId, PageCursorTracer cursorTracer )
    {
        DenseKey key = layout.newKey().initialize( nodeId, type, direction, otherNodeId, internalId );
        try ( RelationshipIterator iterator = new RelationshipIterator( tree.seek( key, key, cursorTracer ), d -> true, bigPropertyValueStore ) )
        {
            if ( iterator.hasNext() )
            {
                return iterator.next();
            }
            return null;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static RelationshipPropertyIterator relationshipPropertiesIterator( ByteBuffer relationshipData, SimpleBigValueStore bigPropertyValueStore )
    {
        if ( relationshipData.remaining() == 0 )
        {
            return NO_PROPERTIES;
        }
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
    }

    Updater newUpdater( PageCursorTracer cursorTracer ) throws IOException
    {
        Writer<DenseKey,DenseValue> writer = tree.writer( cursorTracer );
        return new Updater()
        {
            private final DenseKey key = new DenseKey();
            private final DenseValue value = new DenseValue();

            @Override
            public void createRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing,
                    IntObjectMap<PropertyUpdate> properties, Function<PropertyUpdate,ByteBuffer> version )
            {
                key.initialize( sourceNodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, targetNodeId, internalId );
                value.initialize( properties, version );
                writer.put( key, value );
            }

            @Override
            public void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing )
            {
                key.initialize( sourceNodeId, type, outgoing ? Direction.OUTGOING : Direction.INCOMING, targetNodeId, internalId );
                writer.remove( key );
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

    Stats gatherStats( PageCursorTracer cursorTracer )
    {
        Stats stats = new Stats( tree.sizeInBytes() );
        DenseKey from = new DenseKey();
        DenseKey to = new DenseKey();
        layout.initializeAsLowest( from );
        layout.initializeAsHighest( to );
        try ( Seeker<DenseKey,DenseValue> seek = tree.seek( from, to, cursorTracer ) )
        {
            long nodeId = -1;
            int nodeNumberOfRelationships = 0;
            int nodeByteSize = 0;
            int nodeRelationshipsByteSize = 0;
            while ( seek.next() )
            {
                DenseKey key = seek.key();
                if ( nodeId != key.nodeId )
                {
                    if ( nodeId != -1 )
                    {
                        stats.consume( nodeNumberOfRelationships, nodeByteSize, nodeRelationshipsByteSize );
                        nodeNumberOfRelationships = 0;
                        nodeByteSize = 0;
                        nodeRelationshipsByteSize = 0;
                    }
                    nodeId = key.nodeId;
                }

                int entrySize = layout.keySize( key ) + layout.valueSize( seek.value() );
                nodeByteSize += entrySize;
                nodeNumberOfRelationships++;
                nodeRelationshipsByteSize += entrySize;
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return stats;
    }

    interface Updater extends Closeable
    {
        void createRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> properties,
                Function<PropertyUpdate,ByteBuffer> version );

        void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing );
    }

    private static class DenseLayout extends Layout.Adapter<DenseKey,DenseValue>
    {
        DenseLayout()
        {
            super( false, 989898, 0, 1 );
        }

        @Override
        public DenseKey newKey()
        {
            return new DenseKey();
        }

        @Override
        public DenseKey copyKey( DenseKey key, DenseKey into )
        {
            into.nodeId = key.nodeId;
            into.tokenId = key.tokenId;
            into.direction = key.direction;
            into.internalRelationshipId = key.internalRelationshipId;
            into.neighbourNodeId = key.neighbourNodeId;
            into.cachedSizesByte = key.cachedSizesByte;
            into.cachedKeySize = key.cachedKeySize;
            return into;
        }

        @Override
        public DenseValue newValue()
        {
            return new DenseValue();
        }

        @Override
        public int keySize( DenseKey key )
        {
            return key.getOrCalculateSize();
        }

        @Override
        public int valueSize( DenseValue value )
        {
            return value.data.limit();
        }

        @Override
        public void writeKey( PageCursor cursor, DenseKey key )
        {
            key.getOrCalculateSize();
            cursor.putByte( key.cachedSizesByte );
            writeLongValue( cursor, key.nodeId );
            writeLongValue( cursor, key.tokenAndDirectionInt() );
            writeLongValue( cursor, key.neighbourNodeId );
            writeLongValue( cursor, key.internalRelationshipId );
        }

        @Override
        public void readKey( PageCursor cursor, DenseKey into, int keySize )
        {
            into.cachedSizesByte = cursor.getByte();
            into.nodeId = readLongValue( cursor, into.cachedSizesByte & 0x3 );
            into.fromTokenAndDirection( (int) readLongValue( cursor, (into.cachedSizesByte >>> 2) & 0x3 ) );
            into.neighbourNodeId = readLongValue( cursor, (into.cachedSizesByte >>> 4) & 0x3 );
            into.internalRelationshipId = readLongValue( cursor, (into.cachedSizesByte >>> 6) & 0x3 );
            into.cachedKeySize = keySize;
        }

        @Override
        public void writeValue( PageCursor cursor, DenseValue value )
        {
            cursor.putBytes( value.data.array(), 0, value.data.limit() );
        }

        @Override
        public void readValue( PageCursor cursor, DenseValue into, int valueSize )
        {
            if ( valueSize > MAX_ENTRY_VALUE_SIZE )
            {
                cursor.setCursorException( "Value unreasonably large" );
                return;
            }
            into.data.clear();
            if ( valueSize > 0 )
            {
                cursor.getBytes( into.data.array(), 0, valueSize );
                into.data.position( valueSize );
            }
            into.data.flip();
        }

        @Override
        public void initializeAsLowest( DenseKey key )
        {
            key.nodeId = Long.MIN_VALUE;
        }

        @Override
        public void initializeAsHighest( DenseKey key )
        {
            key.nodeId = Long.MAX_VALUE;
        }

        @Override
        public int compare( DenseKey o1, DenseKey o2 )
        {
            int nodeComparison = Long.compare( o1.nodeId, o2.nodeId );
            if ( nodeComparison != 0 )
            {
                return nodeComparison;
            }
            int tokenIdComparison = Integer.compare( o1.tokenId, o2.tokenId );
            if ( tokenIdComparison != 0 )
            {
                return tokenIdComparison;
            }
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
        }
    }

    private static class DenseKey
    {
        // the order of these fields should match the order in which they are compared
        long nodeId;
        int tokenId;
        Direction direction;
        long neighbourNodeId;
        long internalRelationshipId;

        transient int cachedKeySize;
        transient byte cachedSizesByte;

        DenseKey initialize( long originNodeId, int type, Direction direction, long neighbourNodeId, long internalId )
        {
            this.nodeId = originNodeId;
            this.tokenId = type;
            this.direction = direction;
            this.neighbourNodeId = neighbourNodeId;
            this.internalRelationshipId = internalId;
            this.cachedKeySize = 0;
            return this;
        }

        int getOrCalculateSize()
        {
            if ( cachedKeySize == 0 )
            {
                int nodeIdSize = calculateLongSizeIndex( nodeId );
                int tokenAndDirectionSize = calculateLongSizeIndex( tokenAndDirectionInt() );
                int neighbourIdSize = calculateLongSizeIndex( neighbourNodeId );
                int internalRelationshipIdSize = calculateLongSizeIndex( internalRelationshipId );
                cachedKeySize = 1 + sizeOfLongSizeIndex( nodeIdSize ) + sizeOfLongSizeIndex( tokenAndDirectionSize ) +
                        sizeOfLongSizeIndex( neighbourIdSize ) + sizeOfLongSizeIndex( internalRelationshipIdSize );
                cachedSizesByte = (byte) (nodeIdSize | (tokenAndDirectionSize << 2) | (neighbourIdSize << 4) | (internalRelationshipIdSize << 6));
            }
            return cachedKeySize;
        }

        private int tokenAndDirectionInt()
        {
            return tokenId << 1 | (direction == Direction.OUTGOING ? 1 : 0);
        }

        private void fromTokenAndDirection( int tokenAndDirection )
        {
            tokenId = tokenAndDirection >>> 1;
            direction = (tokenAndDirection & 0x1) != 0 ? Direction.OUTGOING : Direction.INCOMING;
        }

        boolean isOutgoing()
        {
            return direction == Direction.OUTGOING;
        }

        @Override
        public String toString()
        {
            return format( "nodeId:%d,Relationship{type:%d,%s,%d}", nodeId, tokenId, isOutgoing() ? "OUT" : "IN", neighbourNodeId );
        }
    }

    private static class DenseValue
    {
        // TODO for simplicity just have this a ByteBuffer so that the other serialize/deserialize stuff can be used in here too
        // TODO let's just make up some upper limit here and the rest will go to big-value store anyway
        ByteBuffer data = ByteBuffer.wrap( new byte[MAX_ENTRY_VALUE_SIZE] );

        void initialize( IntObjectMap<PropertyUpdate> properties, Function<PropertyUpdate,ByteBuffer> version )
        {
            data.clear();
            if ( !properties.isEmpty() )
            {
                properties.keySet().toSortedArray();
                int[] sortedKeys = properties.keySet().toSortedArray();
                writeIntDeltas( sortedKeys, data );
                for ( int key : sortedKeys )
                {
                    data.put( version.apply( properties.get( key ) ) );
                }
            }
            data.flip();
        }
    }

    private abstract static class CursorIterator<ITEM> extends PrefetchingResourceIterator<ITEM>
    {
        final Seeker<DenseKey,DenseValue> seek;
        private final Predicate<ITEM> filter;
        DenseKey key;
        DenseValue value;

        CursorIterator( Seeker<DenseKey,DenseValue> seek, Predicate<ITEM> filter )
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

    interface RelationshipData
    {
        long internalId();

        long originNodeId();

        long neighbourNodeId();

        int type();

        RelationshipDirection direction();

        Iterator<StorageProperty> properties();

        boolean hasProperties();
    }

    private static class RelationshipIterator extends CursorIterator<RelationshipData> implements RelationshipData
    {
        private final SimpleBigValueStore bigPropertyValueStore;

        RelationshipIterator( Seeker<DenseKey,DenseValue> seek, Predicate<RelationshipData> filter, SimpleBigValueStore bigPropertyValueStore )
        {
            super( seek, filter );
            this.bigPropertyValueStore = bigPropertyValueStore;
        }

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
            return relationshipPropertiesIterator( value.data, bigPropertyValueStore );
        }

        @Override
        public boolean hasProperties()
        {
            return value.data.remaining() > 0;
        }
    }

    private abstract static class RelationshipPropertyIterator extends PrefetchingIterator<StorageProperty> implements StorageProperty
    {
        abstract ByteBuffer serializedValue();
    }

    private static final RelationshipPropertyIterator NO_PROPERTIES = new RelationshipPropertyIterator()
    {
        @Override
        ByteBuffer serializedValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected StorageProperty fetchNextOrNull()
        {
            return null;
        }

        @Override
        public int propertyKeyId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Value value()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDefined()
        {
            throw new UnsupportedOperationException();
        }
    };

    static class Stats
    {
        private final long totalTreeByteSize;
        private long numberOfNodes;
        private long numberOfRelationships;
        private long effectiveRelationshipsByteSize;
        private long effectiveByteSize;

        Stats( long totalTreeByteSize )
        {
            this.totalTreeByteSize = totalTreeByteSize;
        }

        private void consume( int nodeNumberOfRelationships, int nodeByteSize, int nodeRelationshipsByteSize )
        {
            this.numberOfNodes++;
            this.numberOfRelationships += nodeNumberOfRelationships;
            this.effectiveRelationshipsByteSize += nodeRelationshipsByteSize;
            this.effectiveByteSize += nodeByteSize;
        }

        long numberOfRelationships()
        {
            return numberOfRelationships;
        }

        long numberOfNodes()
        {
            return numberOfNodes;
        }

        long effectiveRelationshipsByteSize()
        {
            return effectiveRelationshipsByteSize;
        }

        long effectiveByteSize()
        {
            return effectiveByteSize;
        }

        long totalTreeByteSize()
        {
            return totalTreeByteSize;
        }
    }
}
