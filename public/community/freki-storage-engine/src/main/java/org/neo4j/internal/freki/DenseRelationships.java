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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class DenseRelationships implements Iterable<DenseRelationships.DenseRelationship>
{
    final int type;
    final List<DenseRelationship> relationships = new ArrayList<>();

    DenseRelationships( int type )
    {
        this.type = type;
    }

    void add( long internalId, long sourceNodeId, long otherNodeId, boolean outgoing, IntObjectMap<ByteBuffer> properties )
    {
        relationships.add( new DenseRelationship( internalId, sourceNodeId, otherNodeId, outgoing, properties ) );
    }

    @Override
    public Iterator<DenseRelationship> iterator()
    {
        return relationships.iterator();
    }

    static class DenseRelationship
    {
        long internalId;
        long sourceNodeId;
        long otherNodeId;
        boolean outgoing;
        IntObjectMap<ByteBuffer> properties;

        public DenseRelationship( long internalId, long sourceNodeId, long otherNodeId, boolean outgoing, IntObjectMap<ByteBuffer> properties )
        {
            this.internalId = internalId;
            this.sourceNodeId = sourceNodeId;
            this.otherNodeId = otherNodeId;
            this.outgoing = outgoing;
            this.properties = properties;
        }
    }
}
