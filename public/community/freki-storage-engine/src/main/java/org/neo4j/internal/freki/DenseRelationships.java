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
class DenseRelationships
{
    private final long nodeId;
    final int type;
    // TODO consider consolidating these lists into one with a Mode on each individual relationship?
    final List<DenseRelationship> inserted = new ArrayList<>();
    final List<DenseRelationship> deleted = new ArrayList<>();

    DenseRelationships( long nodeId, int type )
    {
        this.nodeId = nodeId;
        this.type = type;
    }

    void insert( DenseRelationship relationship )
    {
        add( relationship, inserted );
    }

    void delete( DenseRelationship relationship )
    {
        add( relationship, deleted );
    }

    void add( DenseRelationship relationship, List<DenseRelationship> list )
    {
        list.add( relationship );
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
        return type == that.type && Objects.equals( inserted, that.inserted ) && Objects.equals( deleted, that.deleted );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, inserted, deleted );
    }

    static class DenseRelationship
    {
        long internalId;
        long otherNodeId;
        boolean outgoing;
        IntObjectMap<PropertyUpdate> propertyUpdates;

        DenseRelationship( long internalId, long otherNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> propertyUpdates )
        {
            this.internalId = internalId;
            this.otherNodeId = otherNodeId;
            this.outgoing = outgoing;
            this.propertyUpdates = propertyUpdates;
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
                    propertyUpdates.equals( that.propertyUpdates );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( internalId, otherNodeId, outgoing, propertyUpdates );
        }

        @Override
        public String toString()
        {
            return "DenseRelationship{" + "internalId=" + internalId + ", otherNodeId=" + otherNodeId + ", outgoing=" + outgoing + ", propertyUpdates=" +
                    propertyUpdates + '}';
        }
    }
}
