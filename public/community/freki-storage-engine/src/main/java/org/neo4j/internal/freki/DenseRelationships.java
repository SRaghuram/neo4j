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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Logical information about relationships which will go into commands for dense nodes.
 */
class DenseRelationships implements Comparable<DenseRelationships>
{
    private final long nodeId;
    final int type;
    final List<DenseRelationship> relationships = new ArrayList<>();

    DenseRelationships( long nodeId, int type )
    {
        this.nodeId = nodeId;
        this.type = type;
    }
    void add( DenseRelationship relationship )
    {
        relationships.add( relationship );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DenseRelationships that = (DenseRelationships) o;
        return type == that.type && Objects.equals( relationships, that.relationships );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, relationships );
    }

    @Override
    public int compareTo( DenseRelationships o )
    {
        int nodeComparison = Long.compare( nodeId, o.nodeId );
        if ( nodeComparison != 0 )
        {
            return nodeComparison;
        }
        return Integer.compare( type, o.type );
    }

    static class DenseRelationship implements Comparable<DenseRelationship>
    {
        long internalId;
        long otherNodeId;
        boolean outgoing;
        IntObjectMap<PropertyUpdate> propertyUpdates;
        boolean deleted;

        DenseRelationship( long internalId, long otherNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> propertyUpdates, boolean deleted )
        {
            this.internalId = internalId;
            this.otherNodeId = otherNodeId;
            this.outgoing = outgoing;
            this.propertyUpdates = propertyUpdates;
            this.deleted = deleted;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DenseRelationship that = (DenseRelationship) o;
            return internalId == that.internalId && otherNodeId == that.otherNodeId && outgoing == that.outgoing &&
                    propertyUpdates.equals( that.propertyUpdates ) && deleted == that.deleted;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( internalId, otherNodeId, outgoing, propertyUpdates, deleted );
        }

        @Override
        public int compareTo( DenseRelationship o )
        {
            int directionComparison = Boolean.compare( outgoing, o.outgoing );
            if ( directionComparison != 0 )
            {
                return directionComparison;
            }
            int neighbourComparison = Long.compare( otherNodeId, o.otherNodeId );
            if ( neighbourComparison != 0 )
            {
                return neighbourComparison;
            }
            return Long.compare( internalId, o.internalId );
        }

        @Override
        public String toString()
        {
            return "DenseRelationship{" + "internalId=" + internalId + ", otherNodeId=" + otherNodeId + ", outgoing=" + outgoing + ", propertyUpdates=" +
                    propertyUpdates + '}';
        }
    }
}
