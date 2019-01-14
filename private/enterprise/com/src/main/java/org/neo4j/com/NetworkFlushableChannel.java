/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.Flushable;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;

public class NetworkFlushableChannel implements Flushable, FlushableChannel
{
    private final ChannelBuffer delegate;

    public NetworkFlushableChannel( ChannelBuffer delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void flush()
    {
    }

    @Override
    public FlushableChannel put( byte value )
    {
        delegate.writeByte( value );
        return this;
    }

    @Override
    public FlushableChannel putShort( short value )
    {
        delegate.writeShort( value );
        return this;
    }

    @Override
    public FlushableChannel putInt( int value )
    {
        delegate.writeInt( value );
        return this;
    }

    @Override
    public FlushableChannel putLong( long value )
    {
        delegate.writeLong( value );
        return this;
    }

    @Override
    public FlushableChannel putFloat( float value )
    {
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public FlushableChannel putDouble( double value )
    {
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public FlushableChannel put( byte[] value, int length )
    {
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Flushable prepareForFlush()
    {
        return this;
    }
}
