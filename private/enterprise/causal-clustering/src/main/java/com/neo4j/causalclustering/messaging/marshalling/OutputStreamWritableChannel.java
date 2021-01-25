/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.io.fs.WritableChecksumChannel;

public class OutputStreamWritableChannel implements WritableChecksumChannel
{
    private final DataOutputStream dataOutputStream;

    public OutputStreamWritableChannel( OutputStream outputStream )
    {
        this.dataOutputStream = new DataOutputStream( outputStream );
    }

    @Override
    public WritableChecksumChannel put( byte value ) throws IOException
    {
        dataOutputStream.writeByte( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putShort( short value ) throws IOException
    {
        dataOutputStream.writeShort( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putInt( int value ) throws IOException
    {
        dataOutputStream.writeInt( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putLong( long value ) throws IOException
    {
        dataOutputStream.writeLong( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putFloat( float value ) throws IOException
    {
        dataOutputStream.writeFloat( value );
        return this;
    }

    @Override
    public WritableChecksumChannel putDouble( double value ) throws IOException
    {
        dataOutputStream.writeDouble( value );
        return this;
    }

    @Override
    public WritableChecksumChannel put( byte[] value, int length ) throws IOException
    {
        dataOutputStream.write( value, 0, length );
        return this;
    }

    @Override
    public void beginChecksum()
    {
        throw new UnsupportedOperationException( "Checksum is not supported in this implementation." );
    }

    @Override
    public int putChecksum()
    {
        throw new UnsupportedOperationException( "Checksum is not supported in this implementation." );
    }
}
