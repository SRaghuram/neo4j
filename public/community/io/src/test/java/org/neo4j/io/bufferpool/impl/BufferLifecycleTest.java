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
package org.neo4j.io.bufferpool.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class BufferLifecycleTest
{
    private final MemoryTracker memoryTracker = mock( MemoryTracker.class );
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final ArgumentCaptor<Runnable> collectionRunnableCaptor = ArgumentCaptor.forClass( Runnable.class );
    private NeoByteBufferPool bufferPool;

    @BeforeEach
    void setUp()
    {
        var buckets = List.of( "1K:1", "2K:1", "4K:1" );
        var poolConfig = new NeoBufferPoolConfigOverride( null, buckets );
        bufferPool = new NeoByteBufferPool( poolConfig, memoryTracker, jobScheduler );
    }

    @Test
    void testBufferLifecycle() throws Exception
    {
        bufferPool.start();
        verify( jobScheduler ).scheduleRecurring( eq( Group.BUFFER_POOL_MAINTENANCE ), any(), collectionRunnableCaptor.capture(), eq( 20L ),
                eq( TimeUnit.SECONDS ) );
        Runnable collectionRunnable = collectionRunnableCaptor.getValue();

        var b1 = bufferPool.acquire( 1000 );
        var b2 = bufferPool.acquire( 1000 );
        var b3 = bufferPool.acquire( 1000 );
        var b4 = bufferPool.acquire( 2000 );
        var b5 = bufferPool.acquire( 4000 );

        verifyAllocations( 3, 1, 1 );

        collectionRunnable.run();

        // nothing got freed, because the buffers were used since last collection
        verifyReleases( 0, 0, 0 );

        bufferPool.release( b2 );
        bufferPool.release( b3 );
        bufferPool.release( b5 );

        collectionRunnable.run();

        // nothing got freed, because the buffers were used since last collection
        verifyReleases( 0, 0, 0 );

        collectionRunnable.run();

        // now some of the buffer had been sitting idle in the pool since the last collection
        verifyReleases( 2, 0, 1 );

        collectionRunnable.run();
        // nothing changed ...
        verifyReleases( 2, 0, 1 );

        var b6 = bufferPool.acquire( 1000 );
        var b7 = bufferPool.acquire( 1000 );

        verifyAllocations( 5, 1, 1 );

        bufferPool.release( b6 );

        collectionRunnable.run();

        // b6 should be still in the pool, so when we ask for a buffer
        // from the same bucket we should get that one instead of allocation a new one.
        // In other words, this tests the pooling behaviour of the buffer pool.
        var b8 = bufferPool.acquire( 1000 );
        assertSame( b6, b8 );

        collectionRunnable.run();

        // and since b6 was acquired again, it should not have been collected
        verifyReleases( 2, 0, 1 );

        // we are intentionally leaking b1,
        // to check the pool will not release it
        bufferPool.release( b4 );
        bufferPool.release( b7 );
        bufferPool.release( b4 );
        bufferPool.release( b8 );

        bufferPool.stop();

        verifyReleases( 4, 1, 1 );
    }

    private void verifyAllocations( int expected1k, int expected2k, int expected4k )
    {
        verify( memoryTracker, times( expected1k ) ).allocateNative( 1024 );
        verify( memoryTracker, times( expected2k ) ).allocateNative( 2048 );
        verify( memoryTracker, times( expected4k ) ).allocateNative( 4096 );
    }

    private void verifyReleases( int expected1k, int expected2k, int expected4k )
    {
        verify( memoryTracker, times( expected1k ) ).releaseNative( 1024 );
        verify( memoryTracker, times( expected2k ) ).releaseNative( 2048 );
        verify( memoryTracker, times( expected4k ) ).releaseNative( 4096 );
    }
}
