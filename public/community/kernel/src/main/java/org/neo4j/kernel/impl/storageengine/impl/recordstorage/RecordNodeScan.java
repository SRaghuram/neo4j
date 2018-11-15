/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.storageengine.api.AllNodeScan;

/**
 * Maintains state when performing batched all-node scans, potentially from multiple threads.
 * <p>
 * Will break up the scan in ranges depending on the provided size hint.
 */
final class RecordNodeScan implements AllNodeScan
{
    private final AtomicLong nextStart = new AtomicLong( 0 );

    boolean scanBatch( int sizeHint, RecordNodeCursor cursor )
    {
        long start = nextStart.getAndAdd( sizeHint );
        long stopInclusive = start + sizeHint - 1;
        return cursor.scanRange( start, stopInclusive );
    }
}
