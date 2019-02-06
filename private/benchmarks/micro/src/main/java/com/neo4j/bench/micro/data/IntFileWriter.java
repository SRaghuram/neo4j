/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class IntFileWriter implements AutoCloseable
{
    private static final int INTEGERS_PER_BYTE_BUFFER = 10_000;

    private final int integersPerByteBuffer;
    private final Path path;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer;
    private int integersWritten;

    IntFileWriter( Path path ) throws IOException
    {
        this( path, INTEGERS_PER_BYTE_BUFFER );
    }

    IntFileWriter( Path path, int integersPerByteBuffer ) throws IOException
    {
        this.integersPerByteBuffer = integersPerByteBuffer;
        this.path = path;
        this.integersWritten = 0;
        this.fileChannel = FileChannel.open( path, StandardOpenOption.WRITE );
        this.byteBuffer = ByteBuffer.allocate( integersPerByteBuffer * Integer.BYTES );
        byteBuffer.clear();
    }

    public Path path()
    {
        return path;
    }

    public void write( int number ) throws IOException
    {
        byteBuffer.putInt( number );
        // byte buffer is full, write to file
        if ( ++integersWritten % integersPerByteBuffer == 0 )
        {
            writeBufferToChannel();
        }
    }

    private void writeBufferToChannel() throws IOException
    {
        byteBuffer.flip();
        int bytesInBuffer = byteBuffer.limit();
        while ( 0 < bytesInBuffer )
        {
            bytesInBuffer -= fileChannel.write( byteBuffer );
        }
        byteBuffer.clear();
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            writeBufferToChannel();
        }
        finally
        {
            closeFileChannelIfOpen();
        }
    }

    private void closeFileChannelIfOpen() throws IOException
    {
        if ( null != fileChannel && !fileChannel.isOpen() )
        {
            fileChannel.close();
        }
    }

}
