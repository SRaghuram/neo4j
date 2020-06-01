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

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.resourceIterator;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class InMemoryDenseRelationshipTestStore extends LifecycleAdapter implements SimpleDenseRelationshipStore
{
    private final ConcurrentMap<Long,ConcurrentMap<Integer,DenseRelationships>> data = new ConcurrentHashMap<>();
    private SimpleBigValueStore bigValueStore;

    InMemoryDenseRelationshipTestStore( SimpleBigValueStore bigValueStore )
    {
        this.bigValueStore = bigValueStore;
    }

    @Override
    public MutableIntObjectMap<PropertyUpdate> loadRelationshipProperties( long nodeId, long internalId, int type, long otherNodeId, boolean outgoing,
            BiFunction<Integer,ByteBuffer,PropertyUpdate> update, PageCursorTracer cursorTracer )
    {
        DenseRelationships relationships = existingRelationshipsByType( nodeId, type, true );
        DenseRelationships.DenseRelationship relationship = findRelationship( relationships, nodeId, internalId, otherNodeId, outgoing );
        MutableIntObjectMap<PropertyUpdate> updates = IntObjectMaps.mutable.empty();
        relationship.propertyUpdates.forEachKeyValue( ( key, value ) -> updates.put( key, update.apply( key, value.after ) ) );
        return updates;
    }

    @Override
    public ResourceIterator<RelationshipData> getRelationships( long nodeId, int type, Direction direction, PageCursorTracer cursorTracer )
    {
        DenseRelationships relationships = existingRelationshipsByType( nodeId, type, false );
        return relationships == null ? emptyResourceIterator() : resourceIterator(
                relationships.relationships.stream().filter( rel -> directionMatches( rel, direction ) ).map( rel -> (RelationshipData) rel ).iterator(),
                Resource.EMPTY );
    }

    @Override
    public ResourceIterator<RelationshipData> getRelationships( long nodeId, int type, Direction direction, long neighbourNodeId,
            PageCursorTracer cursorTracer )
    {
        DenseRelationships relationships = existingRelationshipsByType( nodeId, type, false );
        return relationships == null ? emptyResourceIterator() : resourceIterator(
                relationships.relationships.stream().filter( rel -> directionMatches( rel, direction ) && rel.otherNodeId == neighbourNodeId )
                        .map( rel -> (RelationshipData) rel ).iterator(), Resource.EMPTY );
    }

    @Override
    public RelationshipData getRelationship( long nodeId, int type, Direction direction, long otherNodeId, long internalId,
            PageCursorTracer cursorTracer )
    {
        return (RelationshipData) findRelationship( existingRelationshipsByType( nodeId, type, true ), nodeId, internalId, otherNodeId,
                direction != Direction.INCOMING );
    }

    @Override
    public Updater newUpdater( PageCursorTracer cursorTracer )
    {
        return new Updater()
        {
            @Override
            public void insertRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing,
                    IntObjectMap<PropertyUpdate> properties, Function<PropertyUpdate,ByteBuffer> version )
            {
                ConcurrentMap<Integer,DenseRelationships> nodeData = data.computeIfAbsent( sourceNodeId, nodeId -> new ConcurrentHashMap<>() );
                InternalRelationship relationship = new InternalRelationship( sourceNodeId, internalId, type, targetNodeId, outgoing, properties );
                nodeData.computeIfAbsent( ANY_RELATIONSHIP_TYPE, t -> new DenseRelationships( sourceNodeId, t ) ).add( relationship );
                nodeData.computeIfAbsent( type, t -> new DenseRelationships( sourceNodeId, t ) ).add( relationship );
            }

            @Override
            public void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing )
            {
                assert type != ANY_RELATIONSHIP_TYPE;
                deleteRelationshipInternal( internalId, sourceNodeId, type, targetNodeId, outgoing );
                deleteRelationshipInternal( internalId, sourceNodeId, ANY_RELATIONSHIP_TYPE, targetNodeId, outgoing );
            }

            private void deleteRelationshipInternal( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing )
            {
                DenseRelationships relationships = existingRelationshipsByType( sourceNodeId, type, true );
                DenseRelationships.DenseRelationship relationship = findRelationship( relationships, sourceNodeId, internalId, targetNodeId, outgoing );
                boolean removed = relationships.relationships.remove( relationship );
                assert removed;
            }

            @Override
            public void close()
            {   // no-op
            }
        };
    }

    private boolean directionMatches( DenseRelationships.DenseRelationship relationship, Direction direction )
    {
        if ( direction == Direction.BOTH )
        {
            return true;
        }
        boolean matchOutgoing = direction == Direction.OUTGOING;
        return matchOutgoing == relationship.outgoing;
    }

    private DenseRelationships existingRelationshipsByType( long nodeId, int type, boolean mustExist )
    {
        ConcurrentMap<Integer,DenseRelationships> nodeData = data.get( nodeId );
        assert nodeData != null;
        DenseRelationships relationships = nodeData.get( type );
        assert !mustExist || relationships != null;
        return relationships;
    }

    private DenseRelationships.DenseRelationship findRelationship( DenseRelationships relationships, long nodeId, long internalId, long otherNodeId,
            boolean outgoing )
    {
        for ( DenseRelationships.DenseRelationship relationship : relationships.relationships )
        {
            if ( relationship.otherNodeId == otherNodeId && relationship.outgoing == outgoing && relationship.internalId == internalId )
            {
                return relationship;
            }
        }
        throw new IllegalArgumentException(
                "No relationship otherNode:" + otherNodeId + ",outgoing:" + outgoing + ",internalId:" + internalId + " found for node:" + nodeId +
                        " with type:" + relationships.type );
    }

    private Value deserializeValue( ByteBuffer buffer )
    {
        return PropertyValueFormat.readEagerly( buffer, bigValueStore, PageCursorTracer.NULL );
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {   // no-op
    }

    @Override
    public Stats gatherStats( PageCursorTracer cursorTracer )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void close() throws IOException
    {   // no-op
    }

    private class InternalRelationship extends DenseRelationships.DenseRelationship implements RelationshipData
    {
        private final long nodeId;
        private final int type;

        InternalRelationship( long nodeId, long internalId, int type, long otherNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> propertyUpdates )
        {
            super( internalId, otherNodeId, outgoing, propertyUpdates, false );
            this.nodeId = nodeId;
            this.type = type;
        }

        @Override
        public long internalId()
        {
            return internalId;
        }

        @Override
        public long originNodeId()
        {
            return nodeId;
        }

        @Override
        public long neighbourNodeId()
        {
            return otherNodeId;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public RelationshipDirection direction()
        {
            return nodeId == otherNodeId ? RelationshipDirection.LOOP : outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING;
        }

        @Override
        public Iterator<StorageProperty> properties()
        {
            return propertyUpdates.stream().map( propertyUpdate -> (StorageProperty) new PropertyKeyValue( propertyUpdate.propertyKeyId,
                    deserializeValue( propertyUpdate.after ) ) ).iterator();
        }

        @Override
        public boolean hasProperties()
        {
            return propertyUpdates.notEmpty();
        }
    }
}
