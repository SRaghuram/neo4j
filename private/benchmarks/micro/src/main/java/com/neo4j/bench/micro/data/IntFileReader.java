/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.neo4j.io.memory.ByteBuffers;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class IntFileReader implements AutoCloseable
{
    private static final int INTEGERS_PER_BYTE_BUFFER = 10_000;

    private final ByteBuffer byteBuffer;
    private final Path path;
    private FileChannel fileChannel;
    private int value = -1;

    static void assertAdvance( IntFileReader intFileReader ) throws IOException
    {
        if ( !intFileReader.advance() )
        {
            throw new IllegalStateException( "Reader returned EOF unexpectedly" );
        }
    }

    IntFileReader( Path path ) throws UncheckedIOException
    {
        this( path, INTEGERS_PER_BYTE_BUFFER );
    }

    IntFileReader( Path path, int integersPerByteBuffer ) throws UncheckedIOException
    {
        this.path = path;
        this.byteBuffer = ByteBuffers.allocate( integersPerByteBuffer * Integer.BYTES, INSTANCE );
        reset();
    }

    void reset()
    {
        try
        {
            closeFileChannelIfOpen();
            fileChannel = FileChannel.open( path, StandardOpenOption.READ );
            tryToRefillBuffer();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    Path path()
    {
        return path;
    }

    int getInt()
    {
        return value;
    }

    boolean advance() throws IOException
    {
        if ( byteBuffer.hasRemaining() )
        {
            value = byteBuffer.getInt();
            return true;
        }
        else
        {
            if ( !tryToRefillBuffer() )
            {
                return false;
            }
            value = byteBuffer.getInt();
            return true;
        }
    }

    private boolean tryToRefillBuffer() throws IOException
    {
        byteBuffer.clear();
        if ( !tryToRefillBuffer( 0 ) )
        {
            return false;
        }
        byteBuffer.flip();
        return true;
    }

    private boolean tryToRefillBuffer( int bytesAlreadyInBuffer ) throws IOException
    {
        int bytesRead = fileChannel.read( byteBuffer );
        if ( -1 == bytesRead )
        {
            return bytesAlreadyInBuffer >= Integer.BYTES;
        }
        else if ( Integer.BYTES > bytesAlreadyInBuffer )
        {
            bytesAlreadyInBuffer += bytesRead;
            return tryToRefillBuffer( bytesAlreadyInBuffer );
        }
        else
        {
            return true;
        }
    }

    @Override
    public void close() throws Exception
    {
        closeFileChannelIfOpen();
    }

    private void closeFileChannelIfOpen() throws IOException
    {
        if ( null != fileChannel && !fileChannel.isOpen() )
        {
            fileChannel.close();
        }
    }
}
