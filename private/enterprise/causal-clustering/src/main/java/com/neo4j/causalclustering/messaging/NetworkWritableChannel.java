/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import org.neo4j.io.fs.WritableChecksumChannel;

public class NetworkWritableChannel implements WritableChecksumChannel, ByteBufBacked
{
    private final ByteBuf delegate;

    public NetworkWritableChannel( ByteBuf byteBuf )
    {
        this.delegate = byteBuf;
    }

    @Override
    public WritableChecksumChannel put( byte value )
    {
        delegate.writeByte( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putShort( short value )
    {
        delegate.writeShort( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putInt( int value )
    {
        delegate.writeInt( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putLong( long value )
    {
        delegate.writeLong( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putFloat( float value )
    {
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putDouble( double value )
    {
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public WritableChecksumChannel put( byte[] value, int length )
    {
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public ByteBuf byteBuf()
    {
        return delegate;
    }

    @Override
    public void beginChecksum()
    {
        // no op
    }

    @Override
    public int putChecksum()
    {
        // no op
        return 0;
    }
}
