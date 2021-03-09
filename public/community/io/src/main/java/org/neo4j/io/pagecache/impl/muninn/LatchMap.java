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
package org.neo4j.io.pagecache.impl.muninn;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.neo4j.util.FeatureToggles;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.BinaryLatch;

/**
 * The LatchMap is used by the {@link MuninnPagedFile} to coordinate concurrent page faults, and ensure that no two
 * threads try to fault in the same page at the same time. If there is high demand for a particular page, then the
 * LatchMap will ensure that only one thread actually does the faulting, and that any other interested threads will
 * wait for the faulting thread to complete the fault before they proceed.
 */
final class LatchMap
{
    static final class Latch extends BinaryLatch
    {
        private LatchMap latchMap;
        private int index;

        @Override
        public void release()
        {
            latchMap.setLatch( index, null );
            super.release();
        }
    }

    static final int DEFAULT_FAULT_LOCK_STRIPING = 128;
    static final int faultLockStriping = FeatureToggles.getInteger( LatchMap.class, "faultLockStriping", DEFAULT_FAULT_LOCK_STRIPING );

    private static final VarHandle LATCHES_ARRAY = MethodHandles.arrayElementVarHandle( Latch[].class );
    private final Latch[] latches;
    private final long faultLockMask;

    LatchMap( int size )
    {
        Preconditions.checkArgument( Integer.bitCount( size ) == 1, "Fault lock stripe count must be a power of 2, was %d", size );
        latches = new Latch[size];
        faultLockMask = size - 1;
    }

    private void setLatch( int index, BinaryLatch newValue )
    {
        LATCHES_ARRAY.setVolatile( latches, index, newValue );
    }

    private boolean compareAndSetLatch( int index, Latch expected, Latch update )
    {
        return LATCHES_ARRAY.compareAndSet( latches, index, expected, update );
    }

    private Latch getLatch( int index )
    {
        return (Latch) LATCHES_ARRAY.getVolatile( latches, index );
    }

    /**
     * If a latch is currently installed for the given (or any colliding) identifier, then it will be waited upon and
     * {@code null} will be returned.
     *
     * Otherwise, if there is currently no latch installed for the given identifier, then one will be created and
     * installed, and that latch will be returned. Once the page fault has been completed, the returned latch must be
     * released. Releasing the latch will unblock all threads that are waiting upon it, and the latch will be
     * atomically uninstalled.
     */
    Latch takeOrAwaitLatch( long identifier )
    {
        int index = index( identifier );
        Latch latch = getLatch( index );
        while ( latch == null )
        {
            latch = new Latch();
            if ( compareAndSetLatch( index, null, latch ) )
            {
                latch.latchMap = this;
                latch.index = index;
                return latch;
            }
            latch = getLatch( index );
        }
        latch.await();
        return null;
    }

    private int index( long identifier )
    {
        return (int) (mix( identifier ) & faultLockMask);
    }

    private long mix( long identifier )
    {
        identifier ^= identifier << 21;
        identifier ^= identifier >>> 35;
        identifier ^= identifier << 4;
        return identifier;
    }
}
