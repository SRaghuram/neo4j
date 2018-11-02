/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static java.lang.Math.min;

public class DataProducer implements ReadableByteChannel
{
    private int bytesLeftToProduce;
    private boolean closed;

    public DataProducer( int size )
    {
        this.bytesLeftToProduce = size;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Already closed" );
        }
        closed = true;
    }

    @Override
    public int read( ByteBuffer dst )
    {
        int toFill = min( dst.remaining(), bytesLeftToProduce );
        int leftToFill = toFill;
        if ( toFill <= 0 )
        {
            return -1;
        }

        while ( leftToFill-- > 0 )
        {
            dst.put( (byte) 5 );
        }
        bytesLeftToProduce -= toFill;
        return toFill;
    }
}
