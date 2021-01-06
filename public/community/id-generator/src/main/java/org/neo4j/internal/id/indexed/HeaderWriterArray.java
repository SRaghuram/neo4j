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
package org.neo4j.internal.id.indexed;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Writes header of an {@link IndexedIdGenerator} into the {@link GBPTree}.
 *
 * @see HeaderReader
 */
class HeaderWriterArray implements Consumer<PageCursor>
{
    /**
     * highId to write in the header. This is a supplier because of how this writer is constructed before entering the critical
     * section inside {@link GBPTree#checkpoint(IOLimiter, PageCursorTracer)} and so the highId may have changed between constructing this writer
     * and entering the checkpoint critical section.
     */
    private final int arraySize;
    private final LongSupplier highId;
    private final LongSupplier highestWrittenId;
    private final long generation;
    private final int idsPerEntry;
    private final int arrayIndex;
    private final int entryOffset = Long.BYTES * 3 + Integer.BYTES;
    private boolean oneIdFile;

    HeaderWriterArray( boolean oneIdFile, int arraySize, int arrayIndex, LongSupplier highId, LongSupplier highestWrittenId, long generation, int idsPerEntry )
    {
        this.arraySize = arraySize;
        this.arrayIndex = arrayIndex;
        this.highId = highId;
        this.highestWrittenId = highestWrittenId;
        this.generation = generation;
        this.idsPerEntry = idsPerEntry;
        this.oneIdFile = oneIdFile;
    }

    @Override
    public void accept( PageCursor cursor )
    {
        if (oneIdFile) {
            long highId = this.highId.getAsLong();
            long highestWrittenId = this.highestWrittenId.getAsLong();
            cursor.putInt(this.arraySize);
            cursor.setOffset(cursor.getOffset() + arrayIndex * entryOffset);
            long startOffset = cursor.getOffset();
            cursor.putLong(highId);
            cursor.putLong(highestWrittenId);
            cursor.putLong(generation);
            cursor.putInt(idsPerEntry);
            long endOffset = cursor.getOffset();
            cursor.setOffset(cursor.getOffset() + (arraySize - arrayIndex - 1) * entryOffset);
            cursor.putInt(-1); // end marker
        }
        else
        {
            long highId = this.highId.getAsLong();
            long highestWrittenId = this.highestWrittenId.getAsLong();
            cursor.putLong( highId );
            cursor.putLong( highestWrittenId );
            cursor.putLong( generation );
            cursor.putInt( idsPerEntry );
        }
    }
}