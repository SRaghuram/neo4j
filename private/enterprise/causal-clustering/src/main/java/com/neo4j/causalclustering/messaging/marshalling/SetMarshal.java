/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public final class SetMarshal
{
    private SetMarshal()
    {
    }

    public static Set<String> unmarshalSet( ReadableChannel channel ) throws IOException
    {
        int size = channel.getInt();
        Set<String> set = new HashSet<>( size );
        for ( int i = 0; i < size; i++ )
        {
            set.add( StringMarshal.unmarshal( channel ) );
        }
        return set;
    }

    public static void marshalSet( WritableChannel channel, Set<String> set ) throws IOException
    {
        channel.putInt( set.size() );
        for ( String value : set )
        {
            StringMarshal.marshal( channel, value );
        }
    }
}
