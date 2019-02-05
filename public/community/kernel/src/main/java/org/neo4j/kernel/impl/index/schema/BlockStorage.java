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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.util.Preconditions;

// TODO potentially remove padding altogether!!!!!
class BlockStorage<KEY, VALUE> implements Closeable
{
    static final int BLOCK_HEADER_SIZE = Long.BYTES  // blockSize
                                       + Long.BYTES; // entryCount

    private final Layout<KEY,VALUE> layout;
    private final FileSystemAbstraction fs;
    private final MutableList<BlockEntry<KEY,VALUE>> bufferedEntries;
    private final ByteBuffer byteBuffer;
    private final Comparator<BlockEntry<KEY,VALUE>> comparator;
    private final StoreChannel storeChannel;
    private final Monitor monitor;
    private final int blockSize;
    private final ByteBufferFactory bufferFactory;
    private File blockFile;
    private int numberOfBlocksInCurrentFile;
    private int currentBufferSize;
    private boolean doneAdding;

    BlockStorage( Layout<KEY,VALUE> layout, ByteBufferFactory bufferFactory, FileSystemAbstraction fs, File blockFile, Monitor monitor, int blockSize )
            throws IOException
    {
        this.layout = layout;
        this.fs = fs;
        this.blockFile = blockFile;
        this.monitor = monitor;
        this.blockSize = blockSize;
        this.bufferedEntries = Lists.mutable.empty();
        this.bufferFactory = bufferFactory;
        this.byteBuffer = bufferFactory.newBuffer( blockSize );
        this.comparator = ( e0, e1 ) -> layout.compare( e0.key(), e1.key() );
        this.storeChannel = fs.create( blockFile );
        resetBufferedEntries();
    }

    public void add( KEY key, VALUE value ) throws IOException
    {
        Preconditions.checkState( !doneAdding, "Cannot add more after done adding" );

        int entrySize = BlockEntry.entrySize( layout, key, value );

        if ( currentBufferSize + entrySize > blockSize )
        {
            // append buffer to file and clear buffers
            flushAndResetBuffer();
            numberOfBlocksInCurrentFile++;
        }

        bufferedEntries.add( new BlockEntry<>( key, value ) );
        currentBufferSize += entrySize;
        monitor.entryAdded( entrySize );
    }

    void doneAdding() throws IOException
    {
        if ( !bufferedEntries.isEmpty() )
        {
            flushAndResetBuffer();
            numberOfBlocksInCurrentFile++;
        }
        doneAdding = true;
        storeChannel.close();
    }

    private void resetBufferedEntries()
    {
        bufferedEntries.clear();
        currentBufferSize = BLOCK_HEADER_SIZE;
    }

    private void flushAndResetBuffer() throws IOException
    {
        bufferedEntries.sortThis( comparator );

        ListBasedBlockEntryCursor<KEY,VALUE> entries = new ListBasedBlockEntryCursor<>( bufferedEntries );
        writeBlock( storeChannel, entries, blockSize, bufferedEntries.size() );

        // Append to file
        monitor.blockFlushed( bufferedEntries.size(), currentBufferSize, storeChannel.position() );
        resetBufferedEntries();
    }

    public void merge() throws IOException
    {
        File sourceFile = blockFile;
        File tempFile = new File( blockFile.getParent(), blockFile.getName() + ".b" );
        try
        {
            int mergeFactor = 10;
            File targetFile = tempFile;
            while ( numberOfBlocksInCurrentFile > 1 )
            {
                // Perform one complete merge iteration, merging all blocks from source into target.
                // After this step, target will contain fewer blocks than source, but may need another merge iteration.
                try ( BlockReader<KEY,VALUE> reader = reader( sourceFile ); StoreChannel targetChannel = fs.open( targetFile, OpenMode.READ_WRITE ) )
                {
                    int blocksMergedSoFar = 0;
                    int blocksInMergedFile = 0;
                    while ( blocksMergedSoFar < numberOfBlocksInCurrentFile )
                    {
                        blocksMergedSoFar += performSingleMerge( mergeFactor, reader, targetChannel );
                        blocksInMergedFile++;
                    }
                    numberOfBlocksInCurrentFile = blocksInMergedFile;
                    monitor.mergeIterationFinished( blocksMergedSoFar, blocksInMergedFile );
                }

                // Flip and restore the channelz
                File tmpSourceFile = sourceFile;
                sourceFile = targetFile;
                targetFile = tmpSourceFile;
            }
        }
        finally
        {
            if ( sourceFile == blockFile )
            {
                fs.deleteFile( tempFile );
            }
            else
            {
                fs.deleteFile( blockFile );
                fs.renameFile( tempFile, blockFile );
            }
        }
    }

    private int performSingleMerge( int mergeFactor, BlockReader<KEY,VALUE> reader, StoreChannel targetChannel )
            throws IOException
    {
        try ( MergingBlockEntryReader<KEY,VALUE> merger = new MergingBlockEntryReader<>( layout ) )
        {
            long blockSize = 0;
            long entryCount = 0;
            int blocksMerged = 0;
            for ( int i = 0; i < mergeFactor; i++ )
            {
                BlockEntryReader<KEY,VALUE> source = reader.nextBlock();
                if ( source != null )
                {
                    blockSize += source.blockSize();
                    entryCount += source.entryCount();
                    blocksMerged++;
                    merger.addSource( source );
                }
                else
                {
                    break;
                }
            }

            writeBlock( targetChannel, merger, blockSize, entryCount );
            monitor.mergedBlocks( blockSize, entryCount, blocksMerged );
            return blocksMerged;
        }
    }

    private void writeBlock( StoreChannel targetChannel, BlockEntryCursor<KEY,VALUE> merger, long blockSize, long entryCount ) throws IOException
    {
        writeHeader( byteBuffer, blockSize, entryCount );
        long actualDataSize = writeMergedBlock( targetChannel, byteBuffer, layout, merger );
        writeLastEntriesWithPadding( targetChannel, byteBuffer, blockSize - actualDataSize );
    }

    private static void writeHeader( ByteBuffer byteBuffer, long blockSize, long entryCount )
    {
        byteBuffer.putLong( blockSize );
        byteBuffer.putLong( entryCount );
    }

    private static <KEY, VALUE> long writeMergedBlock( StoreChannel targetChannel, ByteBuffer byteBuffer, Layout<KEY,VALUE> layout,
            BlockEntryCursor<KEY,VALUE> merger ) throws IOException
    {
        // Loop over block entries
        long actualDataSize = BLOCK_HEADER_SIZE;
        ByteArrayPageCursor pageCursor = new ByteArrayPageCursor( byteBuffer );
        while ( merger.next() )
        {
            KEY key = merger.key();
            VALUE value = merger.value();
            int entrySize = BlockEntry.entrySize( layout, key, value );
            actualDataSize += entrySize;

            if ( byteBuffer.remaining() < entrySize )
            {
                // flush and reset + DON'T PAD!!!
                byteBuffer.flip();
                targetChannel.writeAll( byteBuffer );
                byteBuffer.clear();
            }

            BlockEntry.write( pageCursor, layout, key, value );
        }
        return actualDataSize;
    }

    private static void writeLastEntriesWithPadding( StoreChannel channel, ByteBuffer byteBuffer, long padding ) throws IOException
    {
        boolean didWrite;
        do
        {
            int toPadThisTime = (int) Math.min( byteBuffer.remaining(), padding );
            byte[] padArray = new byte[toPadThisTime];
            byteBuffer.put( padArray );
            padding -= toPadThisTime;
            didWrite = byteBuffer.position() > 0;
            if ( didWrite )
            {
                byteBuffer.flip();
                channel.writeAll( byteBuffer );
                byteBuffer.clear();
            }
        }
        while ( didWrite );
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( storeChannel );
        fs.deleteFile( blockFile );
    }

    BlockReader<KEY,VALUE> reader() throws IOException
    {
        return reader( blockFile );
    }

    BlockEntryCursor<KEY,VALUE> stream() throws IOException
    {
        BlockReader<KEY,VALUE> reader = reader();
        return new BlockEntryCursor<KEY,VALUE>()
        {
            private BlockEntryCursor<KEY,VALUE> block = reader.nextBlock();

            @Override
            public boolean next() throws IOException
            {
                while ( block != null )
                {
                    if ( block.next() )
                    {
                        return true;
                    }
                    block.close();
                    block = reader.nextBlock();
                }
                return false;
            }

            @Override
            public KEY key()
            {
                return block.key();
            }

            @Override
            public VALUE value()
            {
                return block.value();
            }

            @Override
            public void close() throws IOException
            {
                if ( block != null )
                {
                    block.close();
                    block = null;
                }
            }
        };
    }

    private BlockReader<KEY,VALUE> reader( File file ) throws IOException
    {
        return new BlockReader<>( fs, file, layout, blockSize );
    }

    public interface Monitor
    {
        void entryAdded( int entrySize );

        void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush );

        void mergeIterationFinished( int numberOfBlocksBefore, int numberOfBlocksAfter );

        void mergedBlocks( long resultingBlockSize, long resultingEntryCount, int numberOfBlocks );

        class Adapter implements Monitor
        {
            @Override
            public void entryAdded( int entrySize )
            {   // no-op
            }

            @Override
            public void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush )
            {   // no-op
            }

            @Override
            public void mergeIterationFinished( int numberOfBlocksBefore, int numberOfBlocksAfter )
            {   // no-op
            }

            @Override
            public void mergedBlocks( long resultingBlockSize, long resultingEntryCount, int numberOfBlocks )
            {   // no-op
            }
        }

        Monitor NO_MONITOR = new Adapter();
    }
}
