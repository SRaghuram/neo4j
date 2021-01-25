/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChecksumChannel;

public class NetworkReadableChannel implements ReadableClosablePositionAwareChecksumChannel
{
    private final ByteBuf delegate;

    public NetworkReadableChannel( ByteBuf input )
    {
        this.delegate = input;
    }

    @Override
    public byte get() throws IOException
    {
        ensureBytes( Byte.BYTES );
        return delegate.readByte();
    }

    @Override
    public short getShort() throws IOException
    {
        ensureBytes( Short.BYTES );
        return delegate.readShort();
    }

    @Override
    public int getInt() throws IOException
    {
        ensureBytes( Integer.BYTES );
        return delegate.readInt();
    }

    @Override
    public long getLong() throws IOException
    {
        ensureBytes( Long.BYTES );
        return delegate.readLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        ensureBytes( Float.BYTES );
        return delegate.readFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        ensureBytes( Double.BYTES );
        return delegate.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        ensureBytes( length );
        delegate.readBytes( bytes, 0, length );
    }

    private void ensureBytes( int byteCount ) throws ReadPastEndException
    {
        if ( delegate.readableBytes() < byteCount )
        {
            throw ReadPastEndException.INSTANCE;
        }
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker )
    {
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentPosition()
    {
        return LogPosition.UNSPECIFIED;
    }

    @Override
    public void close()
    {
        // no op
    }

    @Override
    public void beginChecksum()
    {
        // no op
    }

    @Override
    public int endChecksumAndValidate()
    {
        // no op
        return 0;
    }
}
