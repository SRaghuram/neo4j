/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.string.UTF8;

class ChannelUtils
{
    static void writeString( String name, FlushableChannel channel ) throws IOException
    {
        byte[] bytes = UTF8.encode( name );
        channel.putInt( bytes.length );
        channel.put( bytes, bytes.length );
    }

    static String readString( ReadableClosableChannel channel ) throws IOException
    {
        byte[] bytes = new byte[channel.getInt()];
        channel.get( bytes, bytes.length );
        return UTF8.decode( bytes );
    }
}
