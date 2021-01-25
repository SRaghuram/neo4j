/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Objects;
import java.util.Queue;

import org.neo4j.io.fs.WritableChecksumChannel;

import static java.lang.Integer.min;

/**
 * Uses provided allocator to create {@link ByteBuf}. The buffers will be split if maximum size is reached. The full buffer is then added
 * to the provided output and a new buffer is allocated. If the output queue is bounded then writing to this channel may block!
 */
public class ChunkingNetworkChannel implements WritableChecksumChannel, AutoCloseable
{
    private static final int DEFAULT_INIT_CHUNK_SIZE = 512;
    private final ByteBufAllocator allocator;
    private final int maxChunkSize;
    private final int initSize;
    private final Queue<ByteBuf> byteBuffs;
    private ByteBuf current;
    private boolean isClosed;

    /**
     * @param allocator used to allocated {@link ByteBuf}
     * @param maxChunkSize when reached the current buffer will be moved to the @param outputQueue and a new {@link ByteBuf} is allocated
     * @param outputQueue full or flushed buffers are added here. If this queue is bounded then writing to this channel may block!
     */
    public ChunkingNetworkChannel( ByteBufAllocator allocator, int maxChunkSize, Queue<ByteBuf> outputQueue )
    {
        Objects.requireNonNull( allocator, "allocator cannot be null" );
        Objects.requireNonNull( outputQueue, "outputQueue cannot be null" );
        this.allocator = allocator;
        this.maxChunkSize = maxChunkSize;
        this.initSize = min( DEFAULT_INIT_CHUNK_SIZE, maxChunkSize );
        if ( maxChunkSize < Double.BYTES )
        {
            throw new IllegalArgumentException( "Chunk size must be at least 8. Got " + maxChunkSize );
        }
        this.byteBuffs = outputQueue;
    }

    @Override
    public WritableChecksumChannel put( byte value )
    {
        checkState();
        prepareWrite( 1 );
        current.writeByte( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putShort( short value )
    {
        checkState();
        prepareWrite( Short.BYTES );
        current.writeShort( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putInt( int value )
    {
        checkState();
        prepareWrite( Integer.BYTES );
        current.writeInt( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putLong( long value )
    {
        checkState();
        prepareWrite( Long.BYTES );
        current.writeLong( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putFloat( float value )
    {
        checkState();
        prepareWrite( Float.BYTES );
        current.writeFloat( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putDouble( double value )
    {
        checkState();
        prepareWrite( Double.BYTES );
        current.writeDouble( value );
        return this;
    }

    @Override
    public WritableChecksumChannel put( byte[] value, int length )
    {
        checkState();
        int writeIndex = 0;
        int remaining = length;
        while ( remaining != 0 )
        {
            int toWrite = prepareGently( remaining );
            ByteBuf current = getOrCreateCurrent();
            current.writeBytes( value, writeIndex, toWrite );
            writeIndex += toWrite;
            remaining = length - writeIndex;
        }
        return this;
    }

    /**
     * Move the current buffer to the output.
     */
    public WritableChecksumChannel flush()
    {
        storeCurrent();
        return this;
    }

    /**
     * @return Provides the writer index of the current (not yet flushed) buffer.
     */
    public int currentIndex()
    {
        return current != null ? current.writerIndex() : 0;
    }

    private int prepareGently( int size )
    {
        if ( getOrCreateCurrent().writerIndex() == maxChunkSize )
        {
            prepareWrite( size );
        }
        return min( maxChunkSize - current.writerIndex(), size );
    }

    private ByteBuf getOrCreateCurrent()
    {
        if ( current == null )
        {
            current = allocateNewBuffer();
        }
        return current;
    }

    private void prepareWrite( int size )
    {
        if ( (getOrCreateCurrent().writerIndex() + size) > maxChunkSize )
        {
            storeCurrent();
        }
        getOrCreateCurrent();
    }

    private void storeCurrent()
    {
        if ( current == null )
        {
            return;
        }
        byteBuffs.add( current );
        current = null;
    }

    private void releaseCurrent()
    {
        if ( this.current != null )
        {
            current.release();
        }
    }

    private ByteBuf allocateNewBuffer()
    {
        return allocator.buffer( initSize, maxChunkSize );
    }

    private void checkState()
    {
        if ( isClosed )
        {
            throw new IllegalStateException( "Channel has been closed already" );
        }
    }

    /**
     * Flushes and closes the channel
     *
     * @see #flush()
     */
    @Override
    public void close()
    {
        try
        {
            flush();
        }
        finally
        {
            isClosed = true;
            releaseCurrent();
        }
    }

    public boolean closed()
    {
        return isClosed;
    }

    @Override
    public void beginChecksum()
    {
        // no op
    }

    @Override
    public int putChecksum()
    {
        return 0; // no op
    }
}
