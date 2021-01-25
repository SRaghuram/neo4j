/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.neo4j.io.fs.ReadableChannel;

public class InputStreamReadableChannel implements ReadableChannel
{
    private final DataInputStream dataInputStream;

    public InputStreamReadableChannel( InputStream inputStream )
    {
        this.dataInputStream = new DataInputStream( inputStream );
    }

    @Override
    public byte get() throws IOException
    {
        return dataInputStream.readByte();
    }

    @Override
    public short getShort() throws IOException
    {
        return dataInputStream.readShort();
    }

    @Override
    public int getInt() throws IOException
    {
        return dataInputStream.readInt();
    }

    @Override
    public long getLong() throws IOException
    {
        return dataInputStream.readLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        return dataInputStream.readFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        return dataInputStream.readDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        dataInputStream.read( bytes, 0, length );
    }

    @Override
    public void close() throws IOException
    {
        dataInputStream.close();
    }
}
