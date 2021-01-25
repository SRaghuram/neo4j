/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.IOException;

import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.string.UTF8;

class ChannelUtils
{
    static void writeString( String name, FlushableChannel channel ) throws IOException
    {
        byte[] bytes = UTF8.encode( name );
        channel.putInt( bytes.length );
        channel.put( bytes, bytes.length );
    }

    static String readString( ReadableChannel channel ) throws IOException
    {
        byte[] bytes = new byte[channel.getInt()];
        channel.get( bytes, bytes.length );
        return UTF8.decode( bytes );
    }
}
