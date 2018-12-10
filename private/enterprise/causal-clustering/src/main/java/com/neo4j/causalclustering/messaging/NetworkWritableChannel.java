/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import org.neo4j.storageengine.api.WritableChannel;

public class NetworkWritableChannel implements WritableChannel, ByteBufBacked
{
    private final ByteBuf delegate;

    public NetworkWritableChannel( ByteBuf byteBuf )
    {
        this.delegate = byteBuf;
    }

    @Override
    public WritableChannel put( byte value )
    {
        delegate.writeByte( value );
        return this;
    }

    @Override
    public WritableChannel putShort( short value )
    {
        delegate.writeShort( value );
        return this;
    }

    @Override
    public WritableChannel putInt( int value )
    {
        delegate.writeInt( value );
        return this;
    }

    @Override
    public WritableChannel putLong( long value )
    {
        delegate.writeLong( value );
        return this;
    }

    @Override
    public WritableChannel putFloat( float value )
    {
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public WritableChannel putDouble( double value )
    {
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public WritableChannel put( byte[] value, int length )
    {
        delegate.writeBytes( value, 0, length );
        return this;
    }

    @Override
    public ByteBuf byteBuf()
    {
        return delegate;
    }
}
