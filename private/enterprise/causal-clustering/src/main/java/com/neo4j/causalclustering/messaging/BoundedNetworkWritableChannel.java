/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class BoundedNetworkWritableChannel implements WritableChannel, ByteBufBacked
{
    /**
     * This implementation puts an upper limit to the size of the state serialized in the buffer. The default
     * value for that should be sufficient for all replicated state except for transactions, the size of which
     * is unbounded.
     */
    private static final long DEFAULT_SIZE_LIMIT = mebiBytes( 2 );

    private final ByteBuf delegate;
    private final int initialWriterIndex;

    private final long sizeLimit;

    public BoundedNetworkWritableChannel( ByteBuf delegate )
    {
        this( delegate, DEFAULT_SIZE_LIMIT );
    }

    public BoundedNetworkWritableChannel( ByteBuf delegate, long sizeLimit )
    {
        this.delegate = delegate;
        this.initialWriterIndex = delegate.writerIndex();
        this.sizeLimit = sizeLimit;
    }

    @Override
    public WritableChannel put( byte value ) throws MessageTooBigException
    {
        checkSize( Byte.BYTES );
        delegate.writeByte( value );
        return this;
    }

    @Override
    public WritableChannel putShort( short value ) throws MessageTooBigException
    {
        checkSize( Short.BYTES );
        delegate.writeShort( value );
        return this;
    }

    @Override
    public WritableChannel putInt( int value ) throws MessageTooBigException
    {
        checkSize( Integer.BYTES );
        delegate.writeInt( value );
        return this;
    }

    @Override
    public WritableChannel putLong( long value ) throws MessageTooBigException
    {
        checkSize( Long.BYTES );
        delegate.writeLong( value );
        return this;
    }

    @Override
    public WritableChannel putFloat( float value ) throws MessageTooBigException
    {
        checkSize( Float.BYTES );
        delegate.writeFloat( value );
        return this;
    }

    @Override
    public WritableChannel putDouble( double value ) throws MessageTooBigException
    {
        checkSize( Double.BYTES );
        delegate.writeDouble( value );
        return this;
    }

    @Override
    public WritableChannel put( byte[] value, int length ) throws MessageTooBigException
    {
        checkSize( length );
        delegate.writeBytes( value, 0, length );
        return this;
    }

    private void checkSize( int additional ) throws MessageTooBigException
    {
        int writtenSoFar = delegate.writerIndex() - initialWriterIndex;
        int countToCheck = writtenSoFar + additional;
        if ( countToCheck > sizeLimit )
        {
            throw new MessageTooBigException( format(
                    "Size limit exceeded. Limit is %d, wanted to write %d with the writer index at %d (started at %d), written so far %d",
                    sizeLimit, additional, delegate.writerIndex(), initialWriterIndex, writtenSoFar ) );
        }
    }

    @Override
    public ByteBuf byteBuf()
    {
        return delegate;
    }
}
