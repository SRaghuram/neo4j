/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * This will produce data like (bytes):
 *
 * 0,1,2,3,4,5,6,7,8,9,0,1,2,3,4... a.s.o.
 *
 * Up until {@code size} number of bytes has been returned.
 *
 */
public class KnownDataByteChannel implements ReadableByteChannel
{
    protected int position;
    private final int size;

    public KnownDataByteChannel( int size )
    {
        this.size = size;
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close()
    {
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int toRead = Math.min( dst.limit() - dst.position(), left() );
        if ( toRead == 0 )
        {
            return -1;
        }

        for ( int i = 0; i < toRead; i++ )
        {
            dst.put( (byte) ((position++) % 10) );
        }
        return toRead;
    }

    private int left()
    {
        return size - position;
    }
}
