/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryStoreStreamProvider implements StoreFileStreamProvider
{
    private Map<String,StringBuffer> fileStreams = new HashMap<>();

    @Override
    public StoreFileStream acquire( String destination, int requiredAlignment )
    {
        fileStreams.putIfAbsent( destination, new StringBuffer() );
        return new InMemoryStoreStream( fileStreams.get( destination ) );
    }

    public Map<String,StringBuffer> fileStreams()
    {
        return fileStreams;
    }

    class InMemoryStoreStream implements StoreFileStream
    {
        private StringBuffer stringBuffer;

        InMemoryStoreStream( StringBuffer stringBuffer )
        {
            this.stringBuffer = stringBuffer;
        }

        @Override
        public void write( ByteBuf data )
        {
            stringBuffer.append( data.toString( UTF_8 ) );
        }

        @Override
        public void close()
        {
            // do nothing
        }
    }
}
