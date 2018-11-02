/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.com.BlockLogBuffer;
import org.neo4j.com.Protocol;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

public class ToNetworkStoreWriter implements StoreWriter
{
    public static final String STORE_COPIER_MONITOR_TAG = "storeCopier";

    private final ChannelBuffer targetBuffer;
    private final ByteCounterMonitor bufferMonitor;

    public ToNetworkStoreWriter( ChannelBuffer targetBuffer, Monitors monitors )
    {
        this.targetBuffer = targetBuffer;
        bufferMonitor = monitors.newMonitor( ByteCounterMonitor.class, getClass().getName(), STORE_COPIER_MONITOR_TAG );
    }

    @Override
    public long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData, int requiredElementAlignment ) throws IOException
    {
        char[] chars = path.toCharArray();
        targetBuffer.writeShort( chars.length );
        Protocol.writeChars( targetBuffer, chars );
        targetBuffer.writeByte( hasData ? 1 : 0 );
        // TODO Make use of temporaryBuffer?
        BlockLogBuffer buffer = new BlockLogBuffer( targetBuffer, bufferMonitor );
        long totalWritten = Short.BYTES + chars.length * Character.BYTES + Byte.BYTES;
        if ( hasData )
        {
            targetBuffer.writeInt( requiredElementAlignment );
            totalWritten += Integer.BYTES;
            totalWritten += buffer.write( data );
            buffer.close();
        }
        return totalWritten;
    }

    @Override
    public void close()
    {
        targetBuffer.writeShort( 0 );
    }
}
