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
package org.neo4j.kernel.impl.util.collection;

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.util.collection.HeapTrackingLongHashSet.createLongHashSet;

class HeapTrackingLongHashSetTest
{
    private MemoryMeter meter = new MemoryMeter();
    private MemoryPool memoryPool;
    private MemoryTracker memoryTracker;

    @BeforeEach
    void setUp()
    {
        memoryPool = MemoryPools.fromLimit( 0 );
        memoryTracker = new LocalMemoryTracker( memoryPool );
    }

    @Test
    void calculateEmptySize()
    {
        HeapTrackingLongHashSet longHashSet = createLongHashSet( memoryTracker );
        assertExactEstimation( longHashSet );
        longHashSet.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void reactToGrowth()
    {
        HeapTrackingLongHashSet longHashSet = createLongHashSet( memoryTracker );
        assertExactEstimation( longHashSet );
        for ( int i = 0; i < 200; i++ )
        {
            longHashSet.add( i );
        }

        assertExactEstimation( longHashSet );
        assertThat( memoryPool.used() ).isGreaterThanOrEqualTo( memoryTracker.estimatedHeapMemory() );

        longHashSet.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory() );

        memoryTracker.reset();
        assertEquals( 0, memoryPool.used() );
    }

    private void assertExactEstimation( LongHashSet longHashSet )
    {
        assertEquals( meter.measureDeep( longHashSet ) - meter.measureDeep( memoryTracker ), memoryTracker.estimatedHeapMemory() );
    }
}
