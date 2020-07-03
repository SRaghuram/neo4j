/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.io.IOUtils.closeAll;

public class StreamToDisk implements StoreFileStream
{
    private StoreChannel storeChannel;
    private List<AutoCloseable> closeables;

    static StreamToDisk fromFile( FileSystemAbstraction fsa, Path file ) throws IOException
    {
        return new StreamToDisk( fsa.write( file.toFile() ) );
    }

    private StreamToDisk( StoreChannel storeChannel, AutoCloseable... closeables )
    {
        this.storeChannel = storeChannel;
        this.closeables = new ArrayList<>();
        this.closeables.add( storeChannel );
        this.closeables.addAll( Arrays.asList( closeables ) );
    }

    @Override
    public void write( ByteBuf data ) throws IOException
    {
        int expectedTotal = data.readableBytes();
        int totalWritten = 0;
        while ( totalWritten < expectedTotal )
        {
            // read from buffer, write to channel
            int bytesWrittenOrEOF = data.readBytes( storeChannel, expectedTotal );
            if ( bytesWrittenOrEOF < 0 )
            {
                throw new IOException( "Unexpected failure writing to channel: " + bytesWrittenOrEOF );
            }
            totalWritten += bytesWrittenOrEOF;
        }
    }

    @Override
    public void close() throws IOException
    {
        closeAll( closeables );
    }
}
