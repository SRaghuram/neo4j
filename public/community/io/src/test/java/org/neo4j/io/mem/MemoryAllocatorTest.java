/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.mem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.LocalMemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MemoryAllocatorTest
{
    private static final String ONE_PAGE = PageCache.PAGE_SIZE + "";
    private static final String EIGHT_PAGES = (8 * PageCache.PAGE_SIZE) + "";

    private MemoryAllocator allocator;

    @AfterEach
    void tearDown()
    {
        closeAllocator();
    }

    @Test
    void allocatedPointerMustNotBeNull()
    {
        MemoryAllocator mman = createAllocator( EIGHT_PAGES );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE, 8 );
        assertThat( address ).isNotEqualTo( 0L );
    }

    @Test
    void allocatedPointerMustBePageAligned()
    {
        MemoryAllocator mman = createAllocator( EIGHT_PAGES );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE, UnsafeUtil.pageSize() );
        assertThat( address % UnsafeUtil.pageSize() ).isEqualTo( 0L );
    }

    @Test
    void allocatedPointerMustBeAlignedToArbitraryByte()
    {
        int pageSize = UnsafeUtil.pageSize();
        for ( int initialOffset = 0; initialOffset < 8; initialOffset++ )
        {
            for ( int i = 0; i < pageSize - 1; i++ )
            {
                MemoryAllocator mman = createAllocator( ONE_PAGE );
                mman.allocateAligned( initialOffset, 1 );
                long alignment = 1 + i;
                long address = mman.allocateAligned( PageCache.PAGE_SIZE, alignment );
                assertThat( address % alignment ).as(
                        "With initial offset " + initialOffset + ", iteration " + i + ", aligning to " + alignment + " and got address " + address ).isEqualTo(
                        0L );
            }
        }
    }

    @Test
    void mustBeAbleToAllocatePastMemoryLimit()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1, 2 ) % 2 ).isEqualTo( 0L );
        }
        // Also asserts that no OutOfMemoryError is thrown.
    }

    @Test
    void allocatedPointersMustBeAlignedPastMemoryLimit()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1, 2 ) % 2 ).isEqualTo( 0L );
        }

        int pageSize = UnsafeUtil.pageSize();
        for ( int i = 0; i < pageSize - 1; i++ )
        {
            int alignment = pageSize - i;
            long address = mman.allocateAligned( PageCache.PAGE_SIZE, alignment );
            assertThat( address % alignment ).as( "iteration " + i + ", aligning to " + alignment ).isEqualTo( 0L );
        }
    }

    @Test
    void alignmentCannotBeZero()
    {
        assertThrows( IllegalArgumentException.class, () -> createAllocator( ONE_PAGE ).allocateAligned( 8, 0 ) );
    }

    @Test
    void mustBeAbleToAllocateSlabsLargerThanGrabSize()
    {
        MemoryAllocator mman = createAllocator( "2 MiB" );
        long page1 = mman.allocateAligned( UnsafeUtil.pageSize(), 1 );
        long largeBlock = mman.allocateAligned( 1024 * 1024, 1 ); // 1 MiB
        long page2 = mman.allocateAligned( UnsafeUtil.pageSize(), 1 );
        assertThat( page1 ).isNotEqualTo( 0L );
        assertThat( largeBlock ).isNotEqualTo( 0L );
        assertThat( page2 ).isNotEqualTo( 0L );
    }

    @Test
    void allocatingMustIncreaseMemoryUsedAndDecreaseAvailableMemory()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        // We haven't allocated anything, so usedMemory should be zero, and the available memory should be the
        // initial capacity.
        assertThat( mman.usedMemory() ).isEqualTo( 0L );
        assertThat( mman.availableMemory() ).isEqualTo( (long) PageCache.PAGE_SIZE );

        // Allocate 32 bytes of unaligned memory. Ideally there would be no memory wasted on this allocation,
        // but in principle we cannot rule it out.
        mman.allocateAligned( 32, 1 );
        assertThat( mman.usedMemory() ).isGreaterThanOrEqualTo( 32L );
        assertThat( mman.availableMemory() ).isLessThanOrEqualTo( PageCache.PAGE_SIZE - 32L );

        // Allocate another 32 bytes of unaligned memory.
        mman.allocateAligned( 32, 1 );
        assertThat( mman.usedMemory() ).isGreaterThanOrEqualTo( 64L );
        assertThat( mman.availableMemory() ).isLessThanOrEqualTo( PageCache.PAGE_SIZE - 64L );

        // Allocate 1 byte to throw off any subsequent accidental alignment.
        mman.allocateAligned( 1, 1 );

        // Allocate 32 bytes memory, but this time it is aligned to a 16 byte boundary.
        mman.allocateAligned( 32, 16 );
        // Don't count the 16 byte alignment in our assertions since we might already be accidentally aligned.
        assertThat( mman.usedMemory() ).isGreaterThanOrEqualTo( 97L );
        assertThat( mman.availableMemory() ).isLessThanOrEqualTo( PageCache.PAGE_SIZE - 97L );
    }

    @Test
    void trackMemoryAllocations()
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        GrabAllocator allocator = (GrabAllocator) MemoryAllocator.createAllocator( "2m", memoryTracker );

        assertEquals( 0, memoryTracker.usedDirectMemory() );

        allocator.allocateAligned( ByteUnit.mebiBytes( 1 ), 1 );

        assertEquals( ByteUnit.mebiBytes( 1 ), memoryTracker.usedDirectMemory() );

        allocator.close();
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }

    private void closeAllocator()
    {
        if ( allocator != null )
        {
            allocator.close();
            allocator = null;
        }
    }

    private MemoryAllocator createAllocator( String expectedMaxMemory )
    {
        closeAllocator();
        allocator = MemoryAllocator.createAllocator( expectedMaxMemory, new LocalMemoryTracker() );
        return allocator;
    }
}
