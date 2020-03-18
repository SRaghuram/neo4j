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
    final List<DenseRelationship> created = new ArrayList<>();
    final List<DenseRelationship> deleted = new ArrayList<>();

    DenseRelationships( long nodeId, int type )
    {
        this.nodeId = nodeId;
        this.type = type;
    }

    void create( DenseRelationship relationship )
    {
        add( relationship, created, 1 );
    }

    void delete( DenseRelationship relationship )
    {
        add( relationship, deleted, -1 );
    }

    void add( DenseRelationship relationship, List<DenseRelationship> list, int increment )
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
        return type == that.type && Objects.equals( created, that.created ) && Objects.equals( deleted, that.deleted );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, created, deleted );
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
    }

    static class Degree
    {
        private int outgoing;
        private int incoming;
        private int loop;

        Degree( int outgoing, int incoming, int loop )
        {
            this.outgoing = outgoing;
            this.incoming = incoming;
            this.loop = loop;
        }

        int outgoing()
        {
            return outgoing;
        }

        int incoming()
        {
            return incoming;
        }

        int loop()
        {
            return loop;
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
            Degree degree = (Degree) o;
            return outgoing == degree.outgoing && incoming == degree.incoming && loop == degree.loop;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( outgoing, incoming, loop );
        }
    }
}
