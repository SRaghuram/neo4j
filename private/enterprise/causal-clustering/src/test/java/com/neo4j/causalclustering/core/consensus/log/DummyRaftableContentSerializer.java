/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class DummyRaftableContentSerializer extends SafeChannelMarshal<ReplicatedContent>
{
    private static final int REPLICATED_INTEGER_TYPE = 0;
    private static final int REPLICATED_STRING_TYPE = 1;

    @Override
    public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
    {
        if ( content instanceof ReplicatedInteger )
        {
            channel.put( (byte) REPLICATED_INTEGER_TYPE );
            channel.putInt( ((ReplicatedInteger) content).get() );
        }
        else if ( content instanceof ReplicatedString )
        {
            String value = ((ReplicatedString) content).get();
            byte[] stringBytes = value.getBytes();
            channel.put( (byte) REPLICATED_STRING_TYPE );
            channel.putInt( stringBytes.length );
            channel.put( stringBytes, stringBytes.length );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type: " + content );
        }
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException
    {
        byte type = channel.get();
        switch ( type )
        {
        case REPLICATED_INTEGER_TYPE:
            return ReplicatedInteger.valueOf( channel.getInt() );
        case REPLICATED_STRING_TYPE:
            int length = channel.getInt();
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            return ReplicatedString.valueOf( new String( bytes ) );
        default:
            throw new IllegalArgumentException( "Unknown content type: " + type );
        }
    }
}
