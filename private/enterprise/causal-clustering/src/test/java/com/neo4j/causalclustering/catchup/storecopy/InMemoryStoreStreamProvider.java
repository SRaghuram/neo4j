/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.HashMap;
import java.util.Map;

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
        public void write( byte[] data )
        {
            for ( byte b : data )
            {
                stringBuffer.append( (char) b );
            }
        }

        @Override
        public void close()
        {
            // do nothing
        }
    }
}
