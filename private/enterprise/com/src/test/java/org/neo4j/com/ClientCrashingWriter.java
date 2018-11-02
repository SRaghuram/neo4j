/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ClientCrashingWriter implements MadeUpWriter
{
    private final MadeUpClient client;
    private final int crashAtSize;
    private int totalSize;

    public ClientCrashingWriter( MadeUpClient client, int crashAtSize )
    {
        this.client = client;
        this.crashAtSize = crashAtSize;
    }

    @Override
    public void write( ReadableByteChannel data )
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1000 );
        while ( true )
        {
            buffer.clear();
            try
            {
                int size = data.read( buffer );
                if ( size == -1 )
                {
                    break;
                }
                if ( (totalSize += size) >= crashAtSize )
                {
                    client.stop();
                    throw new IOException( "Fake read error" );
                }
            }
            catch ( IOException e )
            {
                throw new ComException( e );
            }
        }
    }

    public int getSizeRead()
    {
        return totalSize;
    }
}
