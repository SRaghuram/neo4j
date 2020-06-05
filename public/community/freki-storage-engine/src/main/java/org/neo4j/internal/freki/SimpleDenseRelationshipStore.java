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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageProperty;

public interface SimpleDenseRelationshipStore extends Lifecycle, Closeable
{
    MutableIntObjectMap<PropertyUpdate> loadRelationshipProperties( long nodeId, long internalId, int type, long otherNodeId, boolean outgoing,
            BiFunction<Integer,ByteBuffer,PropertyUpdate> update, PageCursorTracer cursorTracer );

    ResourceIterator<DenseRelationshipStore.RelationshipData> getRelationships( long nodeId, int type, Direction direction, PageCursorTracer cursorTracer );

    ResourceIterator<DenseRelationshipStore.RelationshipData> getRelationships( long nodeId, int type, Direction direction, long neighbourNodeId,
            PageCursorTracer cursorTracer );

    DenseRelationshipStore.RelationshipData getRelationship( long nodeId, int type, Direction direction, long otherNodeId, long internalId,
            PageCursorTracer cursorTracer );

    Updater newUpdater( PageCursorTracer cursorTracer ) throws IOException;

    void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer );

    DenseRelationshipStore.Stats gatherStats( PageCursorTracer cursorTracer );

    interface Updater extends Closeable
    {
        void insertRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> properties,
                Function<PropertyUpdate,ByteBuffer> version );

        void deleteRelationship( long internalId, long sourceNodeId, int type, long targetNodeId, boolean outgoing );
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

    class Stats
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

        void consume( int nodeNumberOfRelationships, int nodeByteSize, int nodeRelationshipsByteSize )
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
