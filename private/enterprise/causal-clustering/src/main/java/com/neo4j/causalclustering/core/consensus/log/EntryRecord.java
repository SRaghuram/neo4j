/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.io.IOException;

import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

/**
 * A log entry and its log index.
 */
public class EntryRecord
{
    private final long logIndex;
    private final RaftLogEntry logEntry;

    public EntryRecord( long logIndex, RaftLogEntry logEntry )
    {
        this.logIndex = logIndex;
        this.logEntry = logEntry;
    }

    public RaftLogEntry logEntry()
    {
        return logEntry;
    }

    public long logIndex()
    {
        return logIndex;
    }

    public static EntryRecord read( ReadableChannel channel, ChannelMarshal<ReplicatedContent> contentMarshal )
            throws IOException, EndOfStreamException
    {
        try
        {
            long appendIndex = channel.getLong();
            long term = channel.getLong();
            ReplicatedContent content = contentMarshal.unmarshal( channel );
            return new EntryRecord( appendIndex, new RaftLogEntry( term, content ) );
        }
        catch ( ReadPastEndException e )
        {
            throw new EndOfStreamException( e );
        }
    }

    public static void write( WritableChannel channel, ChannelMarshal<ReplicatedContent> contentMarshal,
            long logIndex, long term, ReplicatedContent content ) throws IOException
    {
        channel.putLong( logIndex );
        channel.putLong( term );
        contentMarshal.marshal( content, channel );
    }

    @Override
    public String toString()
    {
        return String.format( "%d: %s", logIndex, logEntry );
    }
}
