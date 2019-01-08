/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * The counterpart of {@link BlockLogBuffer}, sits on the receiving end and
 * reads chunks of log. It is provided with a {@link ChannelBuffer} which feeds
 * a series of chunks formatted as follows:
 * <ul>
 * <li>If the first byte is 0, then it is 256 bytes in total size (including the first byte) AND there are more
 * coming.</li>
 * <li>If the first byte is not 0, then its value cast as an
 * integer is the total size of the chunk AND there are no more - the stream is complete</li>
 * </ul>
 */
public class BlockLogReader implements ReadableByteChannel
{
    private final ChannelBuffer source;
    private final byte[] byteArray = new byte[BlockLogBuffer.MAX_SIZE];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap( byteArray );
    private boolean moreBlocks;

    public BlockLogReader( ChannelBuffer source )
    {
        this.source = source;
        readNextBlock();
    }

    /**
     * Read a block from the channel. Read the first byte, determine size and if
     * more are coming, set state accordingly and store content. NOTE: After
     * this op the buffer is flipped, ready to read.
     */
    private void readNextBlock()
    {
        int blockSize = source.readUnsignedByte();
        byteBuffer.clear();
        moreBlocks = blockSize == BlockLogBuffer.FULL_BLOCK_AND_MORE;
        int limit = moreBlocks ? BlockLogBuffer.DATA_SIZE : blockSize;
        byteBuffer.limit( limit );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {
        // This is to make sure that reader index in the ChannelBuffer is left
        // in the right place even if this reader wasn't completely read through.
        readToTheEnd();
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        /*
         * Fill up dst with what comes from the channel, until dst is full.
         * readAsMuchAsPossible() is constantly called reading essentially
         * one chunk at a time until either it runs out of stuff coming
         * from the channel or the actual target buffer is filled.
         */
        int bytesWanted = dst.limit();
        int bytesRead = 0;
        while ( bytesWanted > 0 )
        {
            int bytesReadThisTime = readAsMuchAsPossible( dst, bytesWanted );
            if ( bytesReadThisTime == 0 )
            {
                break;
            }
            bytesRead += bytesReadThisTime;
            bytesWanted -= bytesReadThisTime;
        }
        return bytesRead == 0 && !moreBlocks ? -1 : bytesRead;
    }

    /**
     * Reads in at most {@code maxBytesWanted} in {@code dst} but never more
     * than a chunk.
     *
     * @param dst The buffer to write the reads bytes to
     * @param maxBytesWanted The maximum number of bytes to read.
     * @return The number of bytes actually read
     */
    private int readAsMuchAsPossible( ByteBuffer dst, int maxBytesWanted )
    {
        if ( byteBuffer.remaining() == 0 && moreBlocks )
        {
            readNextBlock();
        }

        int bytesToRead = Math.min( maxBytesWanted, byteBuffer.remaining() );
        dst.put( byteArray, byteBuffer.position(), bytesToRead );
        byteBuffer.position( byteBuffer.position() + bytesToRead );
        return bytesToRead;
    }

    /**
     * Reads everything that can be read from the channel. Stops when a chunk
     * starting with a non zero byte is met.
     */
    private void readToTheEnd()
    {
        while ( moreBlocks )
        {
            readNextBlock();
        }
    }
}
