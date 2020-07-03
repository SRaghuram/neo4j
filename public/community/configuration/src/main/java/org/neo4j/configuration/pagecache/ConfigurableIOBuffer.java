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
package org.neo4j.configuration.pagecache;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.buffer.NativeIOBuffer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.internal.unsafe.UnsafeUtil.allocateMemory;
import static org.neo4j.internal.unsafe.UnsafeUtil.free;
import static org.neo4j.util.FeatureToggles.flag;

public class ConfigurableIOBuffer implements NativeIOBuffer
{
    private static final boolean PRINT_ALLOCATION_EXCEPTION = flag( ConfigurableIOBuffer.class, "printAllocationException", false );
    private static final long NOT_INITIALIZED = 0;
    private final boolean enabled;
    private final MemoryTracker memoryTracker;
    private final long bufferSize;
    private final long bufferAddress;
    private boolean closed;

    public ConfigurableIOBuffer( Config config, MemoryTracker memoryTracker )
    {
        this.memoryTracker = memoryTracker;
        this.bufferSize = PageCache.PAGE_SIZE * config.get( pagecache_flush_buffer_size_in_pages );
        boolean ioBufferEnabled = true;
        long address = NOT_INITIALIZED;
        try
        {
            address = allocateMemory( bufferSize, memoryTracker );
        }
        catch ( Throwable t )
        {
            if ( PRINT_ALLOCATION_EXCEPTION )
            {
                t.printStackTrace();
            }
            ioBufferEnabled = false;
        }
        this.bufferAddress = address;
        this.enabled = ioBufferEnabled;
    }

    @Override
    public boolean isEnabled()
    {
        return enabled;
    }

    @Override
    public boolean hasMoreCapacity( int used, int requestSize )
    {
        if ( !enabled )
        {
            return false;
        }
        return used + requestSize <= bufferSize;
    }

    @Override
    public long getAddress()
    {
        return bufferAddress;
    }

    @Override
    public void close()
    {
        if ( enabled && !closed )
        {
            free( bufferAddress, bufferSize, memoryTracker );
            closed = true;
        }
    }
}
