/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Objects;

public class FileChunk
{
    static final int HEADER_SIZE = Integer.BYTES;

    private static final int HEADER_IS_LAST_FALSE = 0;
    private static final int HEADER_IS_LAST_TRUE = 1;

    private final boolean isLast;
    private final ByteBuf payload;

    static FileChunk create( ByteBuf payload, boolean isLast, int chunkSize )
    {
        if ( !isLast && payload.readableBytes() != chunkSize )
        {
            throw new IllegalArgumentException( "All chunks except for the last must be of max size." );
        }
        return new FileChunk( isLast, payload );
    }

    FileChunk( boolean isLast, ByteBuf payload )
    {
        this.isLast = isLast;
        this.payload = payload;
    }

    public boolean isLast()
    {
        return isLast;
    }

    ByteBuf payload()
    {
        return payload;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FileChunk fileChunk = (FileChunk) o;
        return isLast == fileChunk.isLast && ByteBufUtil.equals( payload, fileChunk.payload );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isLast, payload );
    }

    @Override
    public String toString()
    {
        return "FileChunk{" + "isLast=" + isLast + ", payload=" + payload + '}';
    }

    static int makeHeader( boolean isLast )
    {
        return isLast ? HEADER_IS_LAST_TRUE : HEADER_IS_LAST_FALSE;
    }

    static boolean parseHeader( int header )
    {
        if ( header == HEADER_IS_LAST_TRUE )
        {
            return true;
        }
        else if ( header == HEADER_IS_LAST_FALSE )
        {
            return false;
        }
        else
        {
            throw new IllegalStateException( "Illegal header value: " + header );
        }
    }
}
