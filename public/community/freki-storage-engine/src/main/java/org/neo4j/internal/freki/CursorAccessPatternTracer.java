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

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.tuple.primitive.LongObjectPair;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.neo4j.util.FeatureToggles;

/**
 * Traces how storage cursors are accessed, both which ways are mostly preferred (by reference or direct).
 * The focus is on cursors that spawn from the node cursor, because you reach nodes by reference always (well, except scan).
 * <ul>
 *     <li>node --> property</li>
 *     <li>node --> relationships</li>
 *     <li>relationship -> property</li>
 * </ul>
 */
interface CursorAccessPatternTracer
{
    static CursorAccessPatternTracer decidedByFeatureToggle()
    {
        return FeatureToggles.flag( CursorAccessPatternTracer.class, "enabled", false ) ? new TracingCursorAccessPatternTracer() : NO_TRACING;
    }

    ThreadAccess access();

    void printSummary();

    interface ThreadAccess
    {
        void registerNode( long nodeId );

        void registerNodeToPropertyDirect();

        void registerNodeToPropertyByReference();

        void registerNodeToRelationshipsDirect();

        void registerNodeToRelationshipsByReference();

        void registerRelationshipToPropertyDirect();

        void registerRelationshipToPropertyByReference();

        void registerRelationshipByReference();
    }

    ThreadAccess EMPTY_ACCESS = new ThreadAccess()
    {
        @Override
        public void registerNode( long nodeId )
        {
        }

        @Override
        public void registerNodeToPropertyDirect()
        {
        }

        @Override
        public void registerNodeToPropertyByReference()
        {
        }

        @Override
        public void registerNodeToRelationshipsDirect()
        {
        }

        @Override
        public void registerNodeToRelationshipsByReference()
        {
        }

        @Override
        public void registerRelationshipToPropertyDirect()
        {
        }

        @Override
        public void registerRelationshipToPropertyByReference()
        {
        }

        @Override
        public void registerRelationshipByReference()
        {
        }
    };

    CursorAccessPatternTracer NO_TRACING = new CursorAccessPatternTracer()
    {
        @Override
        public ThreadAccess access()
        {
            return EMPTY_ACCESS;
        }

        @Override
        public void printSummary()
        {
        }
    };

    class TracingCursorAccessPatternTracer implements CursorAccessPatternTracer
    {
        private final Collection<TracingThreadAccess> allTracers = new ConcurrentLinkedQueue<>();
        private final ThreadLocal<ThreadAccess> threadLocalAccess = ThreadLocal.withInitial( () ->
        {
            TracingThreadAccess access = new TracingThreadAccess();
            allTracers.add( access );
            return access;
        } );

        @Override
        public ThreadAccess access()
        {
            return threadLocalAccess.get();
        }

        @Override
        public void printSummary()
        {
            System.out.printf( "Summary of Freki storage cursor access patterns (disable with %s):%n",
                    CursorAccessPatternTracer.class.getName() + ".enabled=false" );
            print( "Node -> property direct", a -> a.nodeToPropertyDirect );
            print( "Node -> property by reference", a -> a.nodeToPropertyByReference );
            print( "Node -> relationships direct", a -> a.nodeToRelationshipsDirect );
            print( "Node -> relationships by reference", a -> a.nodeToRelationshipsByReference );
            print( "Relationship -> property direct", a -> a.relationshipToPropertyDirect );
            print( "Relationship -> property by reference", a -> a.relationshipToPropertyByReference );
            print( "Relationship by reference", a -> a.relationshipByReference );

            LongObjectMap<MutableLong> aggregatedNodeAccesses = aggregateNodeAccesses();
            LongObjectPair<MutableLong>[] nodesAccessesArray = aggregatedNodeAccesses.keyValuesView().toArray( new LongObjectPair[aggregatedNodeAccesses.size()] );
            Arrays.sort( nodesAccessesArray, ( p1, p2 ) -> Long.compare( p2.getTwo().longValue(), p1.getTwo().longValue() ) );
            System.out.println( "Most accessed nodes:" );
            for ( int i = 0; i < nodesAccessesArray.length && i < 10; i++ )
            {
                System.out.printf( "  Node[%d] accessed %d times%n", nodesAccessesArray[i].getOne(), nodesAccessesArray[i].getTwo().longValue() );
            }
            long totalAccesses = Stream.of( nodesAccessesArray ).mapToLong( a -> a.getTwo().longValue() ).sum();
            System.out.printf( "Avg accesses per node %.2f%n", (double) totalAccesses / nodesAccessesArray.length );
        }

        private LongObjectMap<MutableLong> aggregateNodeAccesses()
        {
            Iterator<TracingThreadAccess> accesses = allTracers.iterator();
            if ( !accesses.hasNext() )
            {
                return LongObjectMaps.immutable.empty();
            }

            MutableLongObjectMap<MutableLong> base = accesses.next().nodeAccesses;
            while ( accesses.hasNext() )
            {
                MutableLongObjectMap<MutableLong> other = accesses.next().nodeAccesses;
                other.forEachKeyValue( ( nodeId, value ) ->
                {
                    base.getIfAbsentPut( nodeId, MutableLong::new ).add( value.longValue() );
                } );
            }
            return base;
        }

        private void print( String description, ToLongFunction<TracingThreadAccess> mapper )
        {
            long sum = allTracers.stream().mapToLong( mapper ).sum();
            if ( sum > 0 )
            {
                System.out.printf( "  %s: %d%n", description, sum );
            }
        }
    }

    class TracingThreadAccess implements ThreadAccess
    {
        private MutableLongObjectMap<MutableLong> nodeAccesses = LongObjectMaps.mutable.empty();
        private long nodeToPropertyDirect;
        private long nodeToPropertyByReference;
        private long nodeToRelationshipsDirect;
        private long nodeToRelationshipsByReference;
        private long relationshipToPropertyDirect;
        private long relationshipToPropertyByReference;
        private long relationshipByReference;

        @Override
        public void registerNode( long nodeId )
        {
            nodeAccesses.getIfAbsentPut( nodeId, MutableLong::new ).increment();
        }

        @Override
        public void registerNodeToPropertyDirect()
        {
            nodeToPropertyDirect++;
        }

        @Override
        public void registerNodeToPropertyByReference()
        {
            nodeToPropertyByReference++;
        }

        @Override
        public void registerNodeToRelationshipsDirect()
        {
            nodeToRelationshipsDirect++;
        }

        @Override
        public void registerNodeToRelationshipsByReference()
        {
            nodeToRelationshipsByReference++;
        }

        @Override
        public void registerRelationshipToPropertyDirect()
        {
            relationshipToPropertyDirect++;
        }

        @Override
        public void registerRelationshipToPropertyByReference()
        {
            relationshipToPropertyByReference++;
        }

        @Override
        public void registerRelationshipByReference()
        {
            relationshipByReference++;
        }
    }
}
