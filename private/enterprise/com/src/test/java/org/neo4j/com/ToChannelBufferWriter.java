/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

public class ToChannelBufferWriter implements MadeUpWriter
{
    private final ChannelBuffer target;

    public ToChannelBufferWriter( ChannelBuffer target )
    {
        this.target = target;
    }

    @Override
    public void write( ReadableByteChannel data )
    {
        try ( BlockLogBuffer blockBuffer = new BlockLogBuffer( target, new Monitors().newMonitor( ByteCounterMonitor.class ) ) )
        {
            blockBuffer.write( data );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
