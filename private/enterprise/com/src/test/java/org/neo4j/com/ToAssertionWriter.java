/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static org.junit.Assert.assertEquals;

public class ToAssertionWriter implements MadeUpWriter
{
    private int index;

    @Override
    public void write( ReadableByteChannel data )
    {
        ByteBuffer intermediate = ByteBuffer.allocate( 1000 );
        while ( true )
        {
            try
            {
                intermediate.clear();
                if ( data.read( intermediate ) == -1 )
                {
                    break;
                }
                intermediate.flip();
                while ( intermediate.remaining() > 0 )
                {
                    byte value = intermediate.get();
                    assertEquals( (index++) % 10, value );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
