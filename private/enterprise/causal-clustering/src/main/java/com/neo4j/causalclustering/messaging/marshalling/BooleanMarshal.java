/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class BooleanMarshal
{
    private BooleanMarshal()
    {
    }

    public static boolean unmarshal( ReadableChannel channel ) throws IOException
    {
        return channel.get() != 0;
    }

    public static void marshal( WritableChannel channel, boolean value ) throws IOException
    {
        channel.put( (byte) (value ? 1 : 0 ) );
    }
}
