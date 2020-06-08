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
<<<<<<< HEAD
class DenseRelationships
{
    private final long nodeId;
    final int type;
    // TODO consider consolidating these lists into one with a Mode on each individual relationship?
    final List<DenseRelationship> created = new ArrayList<>();
    final List<DenseRelationship> deleted = new ArrayList<>();
=======
class DenseRelationships implements Comparable<DenseRelationships>
{
    private final long nodeId;
    final int type;
    final List<DenseRelationship> relationships = new ArrayList<>();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

    DenseRelationships( long nodeId, int type )
    {
        this.nodeId = nodeId;
        this.type = type;
    }
<<<<<<< HEAD

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
=======
    void add( DenseRelationship relationship )
    {
        relationships.add( relationship );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
        return type == that.type && Objects.equals( created, that.created ) && Objects.equals( deleted, that.deleted );
=======
        return type == that.type && Objects.equals( relationships, that.relationships );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public int hashCode()
    {
<<<<<<< HEAD
        return Objects.hash( type, created, deleted );
    }

    static class DenseRelationship
=======
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
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        long internalId;
        long otherNodeId;
        boolean outgoing;
        IntObjectMap<PropertyUpdate> propertyUpdates;
<<<<<<< HEAD

        DenseRelationship( long internalId, long otherNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> propertyUpdates )
=======
        boolean deleted;

        DenseRelationship( long internalId, long otherNodeId, boolean outgoing, IntObjectMap<PropertyUpdate> propertyUpdates, boolean deleted )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        {
            this.internalId = internalId;
            this.otherNodeId = otherNodeId;
            this.outgoing = outgoing;
            this.propertyUpdates = propertyUpdates;
<<<<<<< HEAD
=======
            this.deleted = deleted;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
                    propertyUpdates.equals( that.propertyUpdates );
=======
                    propertyUpdates.equals( that.propertyUpdates ) && deleted == that.deleted;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }

        @Override
        public int hashCode()
        {
<<<<<<< HEAD
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
=======
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
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
    }
}
