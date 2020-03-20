/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.bolt.packstream.PackOutput;

public class PackedOutputArray implements PackOutput
{
    private final ByteArrayOutputStream raw;
    private final DataOutputStream data;

    public PackedOutputArray()
    {
        raw = new ByteArrayOutputStream();
        data = new DataOutputStream( raw );
    }

    @Override
    public void beginMessage()
    {
        throw new IllegalStateException( "Not supported" );
    }

    @Override
    public void messageSucceeded()
    {
        throw new IllegalStateException( "Not supported" );
    }

    @Override
    public void messageFailed()
    {
        throw new IllegalStateException( "Not supported" );
    }

    @Override
    public void messageReset()
    {
        throw new IllegalStateException( "Not supported" );
    }

    @Override
    public PackOutput flush()
    {
        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        data.write( value );
        return this;
    }

    @Override
    public PackOutput writeBytes( ByteBuffer buffer ) throws IOException
    {
        while ( buffer.remaining() > 0 )
        {
            data.writeByte( buffer.get() );
        }
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] bytes, int offset, int amountToWrite ) throws IOException
    {
        data.write( bytes, offset, amountToWrite );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        data.writeShort( value );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        data.writeInt( value );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        data.writeLong( value );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        data.writeDouble( value );
        return this;
    }

    @Override
    public void close() throws IOException
    {
        data.close();
    }

    public byte[] bytes()
    {
        return raw.toByteArray();
    }

}
