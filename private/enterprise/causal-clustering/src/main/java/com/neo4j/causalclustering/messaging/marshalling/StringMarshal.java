/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringMarshal
{
    private static final int NULL_STRING_LENGTH = -1;

    private StringMarshal()
    {
    }

    public static void marshal( ByteBuf buffer, String string )
    {
        if ( string == null )
        {
            buffer.writeInt( NULL_STRING_LENGTH );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            buffer.writeInt( bytes.length );
            buffer.writeBytes( bytes );
        }
    }

    public static void marshal( ByteBuffer buffer, String string )
    {
        if ( string == null )
        {
            buffer.putInt( NULL_STRING_LENGTH );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            buffer.putInt( bytes.length );
            buffer.put( bytes );
        }
    }

    public static String unmarshal( ByteBuf buffer )
    {
        int len = buffer.readInt();
        if ( len == NULL_STRING_LENGTH )
        {
            return null;
        }

        byte[] bytes = new byte[len];
        buffer.readBytes( bytes );
        return new String( bytes, UTF_8 );
    }

    public static void marshal( WritableChannel channel, String string ) throws IOException
    {
        if ( string == null )
        {
            channel.putInt( NULL_STRING_LENGTH );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            channel.putInt( bytes.length );
            channel.put( bytes, bytes.length );
        }
    }

    public static String unmarshal( ReadableChannel channel ) throws IOException
    {
        int len = channel.getInt();
        if ( len == NULL_STRING_LENGTH )
        {
            return null;
        }

        byte[] stringBytes = new byte[len];
        channel.get( stringBytes, stringBytes.length );

        return new String( stringBytes, UTF_8 );
    }
}
