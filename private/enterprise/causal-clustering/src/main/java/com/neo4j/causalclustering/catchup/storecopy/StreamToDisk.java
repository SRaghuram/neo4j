/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;

import static org.neo4j.io.IOUtils.closeAll;

public class StreamToDisk implements StoreFileStream
{
    private WritableByteChannel writableByteChannel;
    private List<AutoCloseable> closeables;

    static StreamToDisk fromFile( FileSystemAbstraction fsa, File file ) throws IOException
    {
        return new StreamToDisk( fsa.open( file, OpenMode.READ_WRITE ) );
    }

    private StreamToDisk( WritableByteChannel writableByteChannel, AutoCloseable... closeables )
    {
        this.writableByteChannel = writableByteChannel;
        this.closeables = new ArrayList<>();
        this.closeables.add( writableByteChannel );
        this.closeables.addAll( Arrays.asList( closeables ) );
    }

    @Override
    public void write( byte[] data ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        while ( buffer.hasRemaining() )
        {
            writableByteChannel.write( buffer );
        }
    }

    @Override
    public void close() throws IOException
    {
        closeAll( closeables );
    }
}
