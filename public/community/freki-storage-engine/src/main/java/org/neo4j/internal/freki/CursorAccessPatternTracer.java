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

import static java.lang.Math.max;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;

/**
 * Traces how storage cursors are accessed, both which ways are mostly preferred (by reference or direct).
 * The focus is on cursors that spawn from the node cursor, because you reach nodes by reference always (well, except scan).
 * <ul>
 *     <li>node --> property</li>
 *     <li>node --> relationships</li>
 *     <li>relationship -> property</li>
 * </ul>
 */
public interface CursorAccessPatternTracer
{
    static CursorAccessPatternTracer decidedByFeatureToggle()
    {
        return FeatureToggles.flag( CursorAccessPatternTracer.class, "enabled", false ) ? new TracingCursorAccessPatternTracer() : NO_TRACING;
    }

    ThreadAccess access();

    void clear();

    void printSummary();

    interface ThreadAccess
    {
        void registerNode( long nodeId );

        void registerNodeLabelsAccess();

        void registerNodeToPropertyDirect();

        void registerNodeToPropertyByReference( long nodeId );

        void registerNodeToRelationshipsDirect();

        void registerNodeToRelationshipsByReference( long nodeId );

        void registerRelationshipToPropertyDirect();

        void registerRelationshipToPropertyByReference( long nodeId );

        void registerRelationshipByReference( long relationshipId );
    }

    ThreadAccess EMPTY_ACCESS = new ThreadAccess()
    {
        @Override
        public void registerNode( long nodeId )
        {
        }

        @Override
        public void registerNodeLabelsAccess()
        {
        }

        @Override
        public void registerNodeToPropertyDirect()
        {
        }

        @Override
        public void registerNodeToPropertyByReference( long nodeId )
        {
        }

        @Override
        public void registerNodeToRelationshipsDirect()
        {
        }

        @Override
        public void registerNodeToRelationshipsByReference( long nodeId )
        {
        }

        @Override
        public void registerRelationshipToPropertyDirect()
        {
        }

        @Override
        public void registerRelationshipToPropertyByReference( long nodeId )
        {
        }

        @Override
        public void registerRelationshipByReference( long relationshipId )
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
        public void clear()
        {
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
        public void clear()
        {
            allTracers.forEach( TracingThreadAccess::clear );
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
            System.out.println( "  Highest consecutive loads for same node: " +
                    allTracers.stream().mapToInt( a -> a.highestNumberOfConsecutiveNodeLoads ).max().orElse( -1 ) );
            System.out.println( "  Highest consecutive accesses for same node: " +
                    allTracers.stream().mapToInt( a -> a.highestNumberOfConsecutiveNodeAccesses ).max().orElse( -1 ) );
            System.out.println( "  Avg consecutive loads for same node: " +
                    (double) allTracers.stream().mapToLong( a -> a.numberOfNodeLoads ).sum() /
                    allTracers.stream().mapToLong( a -> a.numberOfConsecutivelyDifferentNodeLoads ).sum() );
            System.out.println( "  Avg consecutive accesses for same node: " +
                    (double) allTracers.stream().mapToLong( a -> a.numberOfAccesses ).sum() /
                    allTracers.stream().mapToLong( a -> a.numberOfConsecutivelyDifferentNodeLoads ).sum() );

            LongObjectMap<MutableLong> aggregatedNodeLoads = aggregateNodeLoads();
            LongObjectPair<MutableLong>[] nodesLoadsArray = aggregatedNodeLoads.keyValuesView().toArray( new LongObjectPair[aggregatedNodeLoads.size()] );
            Arrays.sort( nodesLoadsArray, ( p1, p2 ) -> Long.compare( p2.getTwo().longValue(), p1.getTwo().longValue() ) );
            System.out.println( "Most loaded nodes:" );
            for ( int i = 0; i < nodesLoadsArray.length && i < 10; i++ )
            {
                System.out.printf( "  Node[%d] loaded %d times%n", nodesLoadsArray[i].getOne(), nodesLoadsArray[i].getTwo().longValue() );
            }
            long totalLoads = Stream.of( nodesLoadsArray ).mapToLong( a -> a.getTwo().longValue() ).sum();
            System.out.printf( "Avg loads per node %.2f%n", (double) totalLoads / nodesLoadsArray.length );
        }

        private LongObjectMap<MutableLong> aggregateNodeLoads()
        {
            Iterator<TracingThreadAccess> accesses = allTracers.iterator();
            if ( !accesses.hasNext() )
            {
                return LongObjectMaps.immutable.empty();
            }

            MutableLongObjectMap<MutableLong> base = accesses.next().nodeLoads;
            while ( accesses.hasNext() )
            {
                MutableLongObjectMap<MutableLong> other = accesses.next().nodeLoads;
                other.forEachKeyValue( ( nodeId, value ) -> base.getIfAbsentPut( nodeId, MutableLong::new ).add( value.longValue() ) );
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
        private final MutableLongObjectMap<MutableLong> nodeLoads = LongObjectMaps.mutable.empty();
        private long lastLoadedNodeId = NULL;
        private int numberOfLoadsForSameNode;
        private int numberOfAccessesForSameNode;
        private long numberOfNodeLoads;
        private long numberOfAccesses;
        private long numberOfConsecutivelyDifferentNodeLoads;
        private int highestNumberOfConsecutiveNodeLoads;
        private int highestNumberOfConsecutiveNodeAccesses;

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
            if ( nodeId != lastLoadedNodeId )
            {
                highestNumberOfConsecutiveNodeLoads = max( highestNumberOfConsecutiveNodeLoads, numberOfLoadsForSameNode );
                highestNumberOfConsecutiveNodeAccesses = max( highestNumberOfConsecutiveNodeAccesses, numberOfAccessesForSameNode );
                numberOfLoadsForSameNode = 0;
                numberOfAccessesForSameNode = 0;
                lastLoadedNodeId = nodeId;
                numberOfConsecutivelyDifferentNodeLoads++;
            }
            nodeLoads.getIfAbsentPut( nodeId, MutableLong::new ).increment();
            numberOfLoadsForSameNode++;
            numberOfAccessesForSameNode++;
            numberOfNodeLoads++;
            numberOfAccesses++;
        }

        @Override
        public void registerNodeLabelsAccess()
        {
            numberOfAccessesForSameNode++;
            numberOfAccesses++;
        }

        @Override
        public void registerNodeToPropertyDirect()
        {
            numberOfAccessesForSameNode++;
            nodeToPropertyDirect++;
            numberOfAccesses++;
        }

        @Override
        public void registerNodeToPropertyByReference( long nodeId )
        {
            nodeToPropertyByReference++;
            numberOfAccesses++;
        }

        @Override
        public void registerNodeToRelationshipsDirect()
        {
            numberOfAccessesForSameNode++;
            nodeToRelationshipsDirect++;
            numberOfAccesses++;
        }

        @Override
        public void registerNodeToRelationshipsByReference( long nodeId )
        {
            nodeToRelationshipsByReference++;
            numberOfAccesses++;
        }

        @Override
        public void registerRelationshipToPropertyDirect()
        {
            relationshipToPropertyDirect++;
            numberOfAccesses++;
        }

        @Override
        public void registerRelationshipToPropertyByReference( long nodeId )
        {
            relationshipToPropertyByReference++;
            numberOfAccesses++;
        }

        @Override
        public void registerRelationshipByReference( long relationshipId )
        {
            relationshipByReference++;
            numberOfAccesses++;
        }

        private void clear()
        {
            nodeLoads.clear();
            lastLoadedNodeId = NULL;
            numberOfAccessesForSameNode = 0;
            numberOfLoadsForSameNode = 0;
            numberOfNodeLoads = 0;
            numberOfAccesses++;
            numberOfConsecutivelyDifferentNodeLoads = 0;
            highestNumberOfConsecutiveNodeLoads = 0;
            highestNumberOfConsecutiveNodeAccesses = 0;
            nodeToPropertyDirect = 0;
            nodeToPropertyByReference = 0;
            nodeToRelationshipsDirect = 0;
            nodeToRelationshipsByReference = 0;
            relationshipToPropertyDirect = 0;
            relationshipToPropertyByReference = 0;
            relationshipByReference = 0;
        }
    }
}
