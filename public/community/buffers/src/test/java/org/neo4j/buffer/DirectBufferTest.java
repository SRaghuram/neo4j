/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class DirectBufferTest extends AbstractDirectBufferTest
{

    protected abstract ByteBuf allocate( ByteBufAllocator allocator );

    protected abstract ByteBuf allocate( ByteBufAllocator allocator, int initCapacity );

    protected abstract ByteBuf allocate( ByteBufAllocator allocator, int initCapacity, Integer maxCapacity );

    @Test
    void testBasicAllocation()
    {
        ByteBuf buf = allocate( nettyBufferAllocator, 1500, 10_000 );

        assertEquals( 1500, buf.capacity() );
        assertEquals( 10_000, buf.maxCapacity() );
        assertTrue( buf.isDirect() );

        write( buf, 1000 );
        buf.release();

        assertAcquiredAndReleased( 2048 );
    }

    @Test
    void testAllocationOverPooledCapacity()
    {
        ByteBuf buf = allocate( nettyBufferAllocator, 10_000, 20_000 );

        assertEquals( 10_000, buf.capacity() );
        assertEquals( 20_000, buf.maxCapacity() );

        write( buf, 1000 );
        buf.release();

        assertAcquiredAndReleased( 10_000 );
    }

    @Test
    void testBufferGrow()
    {
        ByteBuf buf = allocate( nettyBufferAllocator, 1500, 30_000 );
        write( buf, 1000 );
        assertEquals( 1500, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 2048, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 4096, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 16_384, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 30_000, buf.capacity() );

        buf.release();

        assertAcquiredAndReleased( 2048, 2048, 4096, 8192, 16_384, 30_000 );
    }

    @Test
    void testDefaultCapacities()
    {
        ByteBuf buf = allocate( nettyBufferAllocator );

        assertEquals( 256, buf.capacity() );
        assertEquals( Integer.MAX_VALUE, buf.maxCapacity() );
        buf.release();

        assertAcquiredAndReleased( 1024 );
    }

    public static class DirectBufferAllocationTest extends DirectBufferTest
    {

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator )
        {
            return allocator.directBuffer();
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity )
        {
            return allocator.directBuffer( initCapacity );
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity, Integer maxCapacity )
        {
            return allocator.directBuffer( initCapacity, maxCapacity );
        }
    }

    public static class DefaultBufferAllocationTest extends DirectBufferTest
    {

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator )
        {
            return allocator.buffer();
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity )
        {
            return allocator.buffer( initCapacity );
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity, Integer maxCapacity )
        {
            return allocator.buffer( initCapacity, maxCapacity );
        }
    }

    public static class IoBufferAllocationTest extends DirectBufferTest
    {

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator )
        {
            return allocator.ioBuffer();
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity )
        {
            return allocator.ioBuffer( initCapacity );
        }

        @Override
        protected ByteBuf allocate( ByteBufAllocator allocator, int initCapacity, Integer maxCapacity )
        {
            return allocator.ioBuffer( initCapacity, maxCapacity );
        }
    }
}
